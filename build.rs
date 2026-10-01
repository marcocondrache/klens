fn main() {
    #[cfg(feature = "ui")]
    memory_serve::load_directory("static");
}
