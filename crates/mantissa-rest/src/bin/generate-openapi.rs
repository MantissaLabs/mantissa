use mantissa_rest::{openapi, server};

/// Regenerates the checked-in Mantissa REST OpenAPI specification.
fn main() -> std::io::Result<()> {
    let document = server::openapi();
    let path = openapi::write_spec_file(&document)?;
    println!("{}", path.display());
    Ok(())
}
