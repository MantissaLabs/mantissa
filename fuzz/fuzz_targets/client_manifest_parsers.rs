#![no_main]

use libfuzzer_sys::fuzz_target;
use mantissa_client::agents::manifest::AgentManifest;
use mantissa_client::jobs::manifest::JobManifest;
use mantissa_client::services::manifest::ServiceManifest;

const MAX_RON_BYTES: usize = 8 * 1024;
const MAX_TOKEN_BYTES: usize = 64;

fuzz_target!(|data: &[u8]| {
    let input = ManifestInput::from_bytes(data);
    input.assert_raw_manifest_parsers_do_not_panic();
    input.assert_generated_valid_manifests();
    input.assert_generated_invalid_manifests_reject();
});

#[derive(Debug)]
struct ManifestInput {
    raw_ron: String,
    name_seed: Vec<u8>,
    image_seed: Vec<u8>,
}

impl ManifestInput {
    /// Maps arbitrary bytes into bounded manifest parser inputs.
    fn from_bytes(data: &[u8]) -> Self {
        let raw_end = data.len().min(MAX_RON_BYTES);
        let raw_ron = String::from_utf8_lossy(&data[..raw_end]).to_string();
        let split = raw_end / 2;

        Self {
            raw_ron,
            name_seed: data[..split].iter().copied().take(MAX_TOKEN_BYTES).collect(),
            image_seed: data[split..raw_end]
                .iter()
                .copied()
                .take(MAX_TOKEN_BYTES)
                .collect(),
        }
    }

    /// Exercises arbitrary RON through each public manifest parser and validator.
    fn assert_raw_manifest_parsers_do_not_panic(&self) {
        if let Ok(manifest) = ron::from_str::<ServiceManifest>(&self.raw_ron) {
            let _ = manifest.validate();
        }
        if let Ok(manifest) = ron::from_str::<JobManifest>(&self.raw_ron) {
            let _ = manifest.validate();
        }
        if let Ok(manifest) = ron::from_str::<AgentManifest>(&self.raw_ron) {
            let _ = manifest.validate();
        }
    }

    /// Verifies generated minimal manifests parse and satisfy validation.
    fn assert_generated_valid_manifests(&self) {
        let name = token("name", &self.name_seed);
        let image = image_name(&self.image_seed);

        let service_ron = format!(
            r#"(name:"service-{name}",tasks:[(name:"web-{name}",image:"{image}")])"#
        );
        let service = ron::from_str::<ServiceManifest>(&service_ron)
            .expect("generated service manifest should parse");
        service
            .validate()
            .expect("generated service manifest should validate");

        let job_ron = format!(r#"(name:"job-{name}",execution:(image:"{image}"))"#);
        let job =
            ron::from_str::<JobManifest>(&job_ron).expect("generated job manifest should parse");
        job.validate()
            .expect("generated job manifest should validate");

        let agent_ron = format!(r#"(name:"agent-{name}",execution:(image:"{image}"))"#);
        let agent = ron::from_str::<AgentManifest>(&agent_ron)
            .expect("generated agent manifest should parse");
        agent
            .validate()
            .expect("generated agent manifest should validate");
    }

    /// Verifies generated manifests with required empty fields reject at validation time.
    fn assert_generated_invalid_manifests_reject(&self) {
        let image = image_name(&self.image_seed);

        let service_ron = format!(r#"(name:"",tasks:[(name:"web",image:"{image}")])"#);
        let service = ron::from_str::<ServiceManifest>(&service_ron)
            .expect("generated invalid service manifest should parse");
        assert!(service.validate().is_err());

        let job_ron = r#"(name:"job",execution:(image:""))"#;
        let job = ron::from_str::<JobManifest>(job_ron)
            .expect("generated invalid job manifest should parse");
        assert!(job.validate().is_err());

        let agent_ron = r#"(name:"agent",execution:(image:""))"#;
        let agent = ron::from_str::<AgentManifest>(agent_ron)
            .expect("generated invalid agent manifest should parse");
        assert!(agent.validate().is_err());
    }
}

/// Builds one manifest-safe identifier from arbitrary bytes.
fn token(prefix: &str, bytes: &[u8]) -> String {
    let mut value = String::from(prefix);
    for byte in bytes.iter().take(MAX_TOKEN_BYTES) {
        let ch = match byte % 37 {
            0..=25 => char::from(b'a' + byte % 26),
            26..=35 => char::from(b'0' + byte % 10),
            _ => '-',
        };
        value.push(ch);
    }
    value
}

/// Builds one image reference that remains non-empty and quote-free.
fn image_name(bytes: &[u8]) -> String {
    format!("registry.local/{}:latest", token("image", bytes))
}
