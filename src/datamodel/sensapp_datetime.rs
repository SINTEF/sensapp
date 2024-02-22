pub type SensAppDateTime = hifitime::Epoch;

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>() {}

    #[test]
    fn test_send() {
        assert_send::<SensAppDateTime>();
    }
}
