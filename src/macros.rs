#[macro_export]
macro_rules! opt {
    ($x:expr) => {
        if let Some(val) = $x {
            val
        }
        else {
            return None;
        }
    }
}