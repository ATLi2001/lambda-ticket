use lambda_http::{run, service_fn, Body, Error, Request, Response};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::AttributeValue;
use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use serde::{Serialize, Deserialize};
use serde_dynamo::to_attribute_value;
use regex::Regex;
use rand_distr::{Normal, Distribution};
use tracing::info;
use std::collections::HashMap;


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Ticket {
    pub id: u32,
    pub taken: bool,
    // reservation details, only filled out if taken=true
    pub res_email: Option<String>,
    pub res_name: Option<String>,
    pub res_card: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Item {
    pub key: String,
    pub id: String,
    pub version: u32,
    pub value: Ticket,
}

fn as_string(val: Option<&AttributeValue>, default: &String) -> String {
    if let Some(v) = val {
        if let Ok(s) = v.as_s() {
            return s.to_owned();
        }
    }
    default.to_owned()
}

fn as_u32(val: Option<&AttributeValue>, default: u32) -> u32 {
    if let Some(v) = val {
        if let Ok(n) = v.as_n() {
            if let Ok(n) = n.parse::<u32>() {
                return n;
            }
        }
    }
    default
}

fn as_bool(val: Option<&AttributeValue>, default: bool) -> bool {
    if let Some(v) = val {
        if let Ok(b) = v.as_bool() {
            return b.to_owned();
        }
    }
    default
}

fn as_ticket(val: Option<&AttributeValue>) -> Ticket {
    let ticket_m = val.unwrap().as_m().unwrap();
    ticket_m.into()
}

impl From<&HashMap<String, AttributeValue>> for Item {
    fn from(value: &HashMap<String, AttributeValue>) -> Self {
        Item {
            key: as_string(value.get("Key"), &"".to_string()),
            id: as_string(value.get("ID"), &"".to_string()),
            version: as_u32(value.get("Version"), 0),
            value: as_ticket(value.get("Value")),
        }
    }
}

impl From<&HashMap<String, AttributeValue>> for Ticket {
    fn from(value: &HashMap<String, AttributeValue>) -> Self {
        Ticket {
            id: as_u32(value.get("id"), 0),
            taken: as_bool(value.get("taken"), false),
            res_email: Some(as_string(value.get("res_email"), &"".to_string())),
            res_name: Some(as_string(value.get("res_name"), &"".to_string())),
            res_card: Some(as_string(value.get("res_card"), &"".to_string())),
        }
    }
}


// reserve a ticket
async fn handle_request(db_client: &Client, event: Request) -> Result<Response<Body>, Error> {
    const TABLE_NAME: &str = "Radical-Ticket";
    
    // Extract some useful information from the request
    let body = event.body();
    let s = std::str::from_utf8(body).expect("invalid utf-8 sequence");
    // Log into Cloudwatch
    info!(payload = %s, "JSON Payload received");

    // Serialze JSON into struct.
    // If JSON is incorrect, send back 400 with error.
    let ticket = match serde_json::from_str::<Ticket>(s) {
        Ok(ticket) => ticket,
        Err(err) => {
            let resp = Response::builder()
                .status(400)
                .header("content-type", "text/html")
                .body(err.to_string().into())
                .map_err(Box::new)?;
            return Ok(resp);
        }
    };

    let ticket_id = ticket.id;

    // get old val to compute new version number
    let old_item = get_item(db_client, &format!("ticket-{ticket_id}"), TABLE_NAME).await?;
    let new_version = old_item.version + 1;

    if old_item.value.taken {
        let resp = Response::builder()
            .status(500)
            .body("ticket already reserved".into())
            .map_err(Box::new)?;
        return Ok(resp);
    }

    let new_ticket = Ticket {
        id: ticket_id,
        taken: true,
        res_email: Some(ticket.res_email.expect("no reservation email")),
        res_name: Some(ticket.res_name.expect("no reservation name")),
        res_card: Some(ticket.res_card.expect("no reservation card")),
    };

    // call anti fraud detection
    if !anti_fraud(&new_ticket) {
        let resp = Response::builder()
            .status(500)
            .header("content-type", "text/html")
            .body("fraudulent reservation detected".into())
            .map_err(Box::new)?;
        return Ok(resp);
    }

    let new_item = Item {
        key: format!("ticket-{ticket_id}"),
        id: format!("ticket-{ticket_id}"),
        version: new_version,
        value: new_ticket,
    };
    
    // Insert into the table.
    add_item(db_client, new_item.clone(), TABLE_NAME).await?;

    // Send back a 200 - success
    let resp = Response::builder()
        .status(200)
        .header("content-type", "text/html")
        .body("Success".into())
        .map_err(Box::new)?;
    Ok(resp)
}


// get an item from a table
pub async fn get_item(client: &Client, key: &str, table: &str) -> Result<Item, Error> {
    let key_av = to_attribute_value(key)?;
    let request = client
        .get_item()
        .table_name(table)
        .key("Key", key_av);

    let resp = request.send().await?;

    Ok(resp.item().unwrap().into())
}

// Add an item to a table.
// snippet-start:[dynamodb.rust.add-item]
pub async fn add_item(client: &Client, item: Item, table: &str) -> Result<(), Error> {
    let key_av = to_attribute_value(item.key)?;
    let id_av = to_attribute_value(item.id)?;
    let version_av = to_attribute_value(item.version)?;
    let value_av = to_attribute_value(item.value)?;

    let request = client
        .put_item()
        .table_name(table)
        .item("Key", key_av)
        .item("ID", id_av)
        .item("Version", version_av)
        .item("Value", value_av);

    info!("adding item to DynamoDB");

    let _resp = request.send().await?;

    Ok(())
}

// // create new tickets 
// // expected request body with number
// async fn populate_tickets(mut req: Request, _ctx: RouteContext<()>) -> Result<Response> {
//     // extract request number
//     let n = req.text().await?.parse::<u32>().unwrap();

//     let cache = CacheKV::new().await;

//     // create n tickets 
//     for i in 0..n {
//         let ticket = Ticket { 
//             id: i,
//             taken: false,
//             res_email: None,
//             res_name: None,
//             res_card: None,
//         };
//         let val = MyValue {
//             version: 0,
//             value: ticket,
//         };

//         cache.put(&format!("ticket-{i}"), &val).await?;
//     }
    
//     // save in cache so we can know how much to clear later
    
//     cache.put("count", &n).await?;

//     Response::ok("")
// }

// // clear the entire cache of tickets
// async fn clear_cache(_req:Request, _ctx: RouteContext<()>) -> Result<Response> {
//     let cache = CacheKV::new().await;
//     // get count of how many tickets total
//     let n = cache.get::<u32>("count").await?.unwrap();

//     for i in 0..n {
//         cache.delete(&format!("ticket-{i}")).await?;
//     }

//     // reset count
//     cache.put("count", &0).await?;

//     Response::ok("Successfully cleared cache")
// }

// // return a specific ticket
// async fn get_ticket(_req: Request, ctx: RouteContext<()>) -> Result<Response> {
//     if let Some(ticket_id) = ctx.param("id") {
//         let id = ticket_id.parse::<u32>().expect("ticket id should be a number");
//         let cache = CacheKV::new().await;
//         match cache.get::<MyValue>(&format!("ticket-{id}")).await? {
//             Some(val) => {
//                 Response::from_json(&val.value)
//             },
//             None => {
//                 Response::error("not found", 500)
//             }
//         }
//     } 
//     else {
//         Response::error("Bad request", 400)
//     }
// }

// multiply an input vector by a random normal matrix, returning an output vector
fn multiply_random_normal(input_vec: Vec<f32>, output_dim: usize, scale: f32) -> Vec<f32> {
    let normal = Normal::new(0.0, scale).unwrap();

    let mut normal_matrix = vec![vec![0f32; input_vec.len()]; output_dim];
    for i in 0..normal_matrix.len() {
        for j in 0..normal_matrix[i].len() {
            normal_matrix[i][j] = normal.sample(&mut rand::thread_rng());
        }
    }

    // output = (normal_matrix)(input_vec)
    let mut output = vec![0f32; output_dim];
    for i in 0..output.len() {
        for j in 0..input_vec.len() {
            output[i] += normal_matrix[i][j] * input_vec[j];
        }
    }

    output
}

// compute relu of input vector
fn relu(input_vec: Vec<f32>) -> Vec<f32> {
    let mut output = vec![0f32; input_vec.len()];
    for i in 0..output.len() {
        if input_vec[i] > 0.0 {
            output[i] = input_vec[i];
        }
    }

    output
}

// check if ticket reservation passes anti fraud test
// true means reservation is ok, false means not
fn anti_fraud(ticket: &Ticket) -> bool {
    // valid email must have some valid characters before @, some after, a dot, then some more
    const EMAIL_REGEX: &str = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)";
    let re = Regex::new(EMAIL_REGEX).unwrap();

    if !re.is_match(&ticket.res_email.clone().unwrap()) {
        return false;
    }

    // check "ml" model
    // create a feature vector from the name, email, and card
    let feature_str = [
        ticket.res_name.clone().unwrap().as_bytes(), 
        ticket.res_email.clone().unwrap().as_bytes(), 
        ticket.res_card.clone().unwrap().as_bytes(),
    ].concat();
    // feature vector is normalized
    let mut feature_vec = vec![0f32; feature_str.len()];
    let mut feature_norm = 0.0;
    for i in 0..feature_str.len() {
        feature_norm += (feature_str[i] as f32).powi(2);
    }
    for i in 0..feature_vec.len() {
        feature_vec[i] = (feature_str[i] as f32) / (feature_norm.sqrt());
    }

    let model_depth = 50;
    for i in 0..model_depth {
        feature_vec = multiply_random_normal(feature_vec, 128, (i+1) as f32);
        feature_vec = relu(feature_vec);
    }

    true
}

// // reserve a ticket
// // expect request as a json with form of Ticket
// async fn reserve_ticket(mut req: Request, _ctx: RouteContext<()>) -> Result<Response> {
//     let ticket = req.json::<Ticket>().await?;
//     let ticket_id = ticket.id;

//     let cache = CacheKV::new().await;

//     // get old val to compute new version number
//     let resp = cache.get::<MyValue>(&format!("ticket-{ticket_id}")).await?;
//     if resp.is_none() {
//         return Response::error("not found", 500);
//     }
    
//     let old_val = resp.unwrap();
//     let new_version = old_val.version + 1;
//     // check that the ticket is not already taken
//     if old_val.value.taken {
//         return Response::error("ticket already reserved", 500);
//     }
    
//     // create new ticket that is taken while checking reservation details are given
//     let new_ticket = Ticket {
//         id: ticket_id,
//         taken: true,
//         res_email: Some(ticket.res_email.expect("no reservation email")),
//         res_name: Some(ticket.res_name.expect("no reservation name")),
//         res_card: Some(ticket.res_card.expect("no reservation card")),
//     };

//     // call anti fraud detection
//     if !anti_fraud(&new_ticket) {
//         return Response::error("fraudulent reservation detected", 500);
//     }

//     let new_val = MyValue {
//         version: new_version,
//         value: new_ticket,
//     };

//     // put back into cache
//     cache.put(&format!("ticket-{ticket_id}"), &new_val).await?;

//     Response::ok("Success")
// }


#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    //Get config from environment.
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-2");
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    //Create the DynamoDB client.
    let client = Client::new(&config);

    run(service_fn(|event: Request| async {
        handle_request(&client, event).await
    }))
    .await
}
