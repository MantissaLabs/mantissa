use crate::ingress::types::{IngressPoolSpecValue, IngressPoolSpreadKey};
use crate::workload::capnp_codec::{decode_placement_policy, encode_placement_policy};
use capnp::message::ReaderOptions;
use capnp::{Error, serialize};
use mantissa_protocol::ingress::{ingress_pool_spec, ingress_pool_spread_key};
use mantissa_store::codec::StoreValueCodec;
use std::io::Cursor;
use uuid::Uuid;

impl StoreValueCodec for IngressPoolSpecValue {
    /// Encodes one ingress pool spec as the stable Cap'n Proto store value.
    fn encode_store_value(&self) -> mantissa_store::Result<Vec<u8>> {
        let mut message = capnp::message::Builder::new_default();
        write_ingress_pool_spec(message.init_root::<ingress_pool_spec::Builder<'_>>(), self)
            .map_err(ingress_pool_store_codec_error)?;
        Ok(serialize::write_message_to_words(&message))
    }

    /// Decodes one ingress pool spec from the stable Cap'n Proto store value.
    fn decode_store_value(bytes: &[u8]) -> mantissa_store::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let reader = serialize::read_message(&mut cursor, ReaderOptions::new())
            .map_err(ingress_pool_store_codec_error)?;
        let spec = reader
            .get_root::<ingress_pool_spec::Reader<'_>>()
            .map_err(ingress_pool_store_codec_error)?;
        read_ingress_pool_spec(spec).map_err(ingress_pool_store_codec_error)
    }
}

/// Encodes one ingress pool spec into the shared store schema.
pub(crate) fn write_ingress_pool_spec(
    mut builder: ingress_pool_spec::Builder<'_>,
    value: &IngressPoolSpecValue,
) -> Result<(), Error> {
    builder.set_id(value.id.as_bytes());
    builder.set_name(&value.name);
    builder.set_min_nodes(value.min_nodes);
    builder.set_max_nodes(value.max_nodes.unwrap_or(0));
    encode_placement_policy(builder.reborrow().init_placement(), &value.placement);
    write_spread_key(
        builder.reborrow().init_spread_by(),
        value.spread_by.as_ref(),
    );
    builder.set_generation(value.generation);
    builder.set_created_at(&value.created_at);
    builder.set_updated_at(&value.updated_at);
    Ok(())
}

/// Decodes one ingress pool spec from the shared store schema.
pub(crate) fn read_ingress_pool_spec(
    reader: ingress_pool_spec::Reader<'_>,
) -> Result<IngressPoolSpecValue, Error> {
    let raw_id = reader.get_id()?;
    if raw_id.len() != 16 {
        return Err(Error::failed(format!(
            "ingress pool id must be 16 bytes, got {}",
            raw_id.len()
        )));
    }
    let mut id = [0u8; 16];
    id.copy_from_slice(raw_id);
    let max_nodes = match reader.get_max_nodes() {
        0 => None,
        value => Some(value),
    };
    Ok(IngressPoolSpecValue {
        id: Uuid::from_bytes(id),
        name: reader.get_name()?.to_str()?.to_string(),
        min_nodes: reader.get_min_nodes(),
        max_nodes,
        placement: decode_placement_policy(reader.get_placement()?)?,
        spread_by: read_spread_key(reader.get_spread_by()?)?,
        generation: reader.get_generation(),
        created_at: reader.get_created_at()?.to_str()?.to_string(),
        updated_at: reader.get_updated_at()?.to_str()?.to_string(),
    })
}

/// Encodes an optional spread key into the ingress pool store schema.
pub(crate) fn write_spread_key(
    mut builder: ingress_pool_spread_key::Builder<'_>,
    spread_by: Option<&IngressPoolSpreadKey>,
) {
    match spread_by {
        Some(IngressPoolSpreadKey::NodeLabel { key }) => builder.set_node_label(key),
        None => builder.set_none(()),
    }
}

/// Decodes an optional spread key from the ingress pool store schema.
pub(crate) fn read_spread_key(
    reader: ingress_pool_spread_key::Reader<'_>,
) -> Result<Option<IngressPoolSpreadKey>, Error> {
    match reader.which() {
        Ok(ingress_pool_spread_key::Which::NodeLabel(Ok(key))) => {
            let key = key.to_str()?.to_string();
            Ok(Some(IngressPoolSpreadKey::NodeLabel { key }))
        }
        Ok(ingress_pool_spread_key::Which::NodeLabel(Err(err))) => Err(err),
        _ => Ok(None),
    }
}

/// Converts Cap'n Proto codec failures into the store error type.
fn ingress_pool_store_codec_error(
    error: impl std::fmt::Display,
) -> Box<mantissa_store::error::Error> {
    Box::new(mantissa_store::error::Error::Other(format!(
        "ingress pool store codec error: {error}"
    )))
}
