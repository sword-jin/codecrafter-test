use rand::Rng;
use rand::distributions::Alphanumeric;

pub fn generate_id() -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(40)
        .map(char::from)
        .collect()
}