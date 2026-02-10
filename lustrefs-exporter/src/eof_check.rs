#[cfg(test)]
mod tests {
    use prometheus_client::{encoding::text::encode, registry::Registry, metrics::counter::Counter};

    #[test]
    fn test_encode_eof() {
        let mut registry = Registry::default();
        let counter: Counter = Counter::default();
        registry.register("my_counter", "A counter", counter);

        let mut buffer = String::new();
        encode(&mut buffer, &registry).unwrap();

        println!("Encoded output:\n{}", buffer);
        assert!(buffer.ends_with("# EOF\n"));
    }
}
