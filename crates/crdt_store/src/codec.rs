use std::io::Cursor;
use uuid::Uuid;

use crate::error::Error;
use crate::mvreg::{MvReg, MvRegEntry, VectorClock};
use crate::store_capnp;

/// Codec implemented by domain values stored inside Cap'n Proto register rows.
pub trait StoreValueCodec: Sized {
    /// Encodes one domain value into its stable store payload bytes.
    fn encode_store_value(&self) -> crate::Result<Vec<u8>>;

    /// Decodes one domain value from its stable store payload bytes.
    fn decode_store_value(bytes: &[u8]) -> crate::Result<Self>;
}

impl StoreValueCodec for Vec<u8> {
    fn encode_store_value(&self) -> crate::Result<Vec<u8>> {
        Ok(self.clone())
    }

    fn decode_store_value(bytes: &[u8]) -> crate::Result<Self> {
        Ok(bytes.to_vec())
    }
}

impl StoreValueCodec for String {
    fn encode_store_value(&self) -> crate::Result<Vec<u8>> {
        Ok(self.as_bytes().to_vec())
    }

    fn decode_store_value(bytes: &[u8]) -> crate::Result<Self> {
        String::from_utf8(bytes.to_vec())
            .map_err(|error| Box::new(Error::Other(format!("invalid UTF-8 store string: {error}"))))
    }
}

/// Codec implemented by actor identifiers persisted in register clocks.
pub trait StoreActorCodec: Clone + Ord + Sized {
    /// Encodes one actor identifier into stable store bytes.
    fn encode_store_actor(&self) -> Vec<u8>;

    /// Decodes one actor identifier from stable store bytes.
    fn decode_store_actor(bytes: &[u8]) -> crate::Result<Self>;
}

impl StoreActorCodec for Uuid {
    fn encode_store_actor(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    fn decode_store_actor(bytes: &[u8]) -> crate::Result<Self> {
        Uuid::from_slice(bytes).map_err(|error| {
            Box::new(Error::Other(format!(
                "invalid store actor bytes: expected 16-byte UUID actor: {error}"
            )))
        })
    }
}

/// Codec implemented by concrete CRDT register row formats.
pub trait StoreRegisterCodec {
    type Reg;

    /// Encodes one register into its stable store row bytes.
    fn encode_store_reg(reg: &Self::Reg) -> crate::Result<Vec<u8>>;

    /// Decodes one register from stable store row bytes.
    fn decode_store_reg(bytes: &[u8]) -> crate::Result<Self::Reg>;
}

/// Store-register codec for Mantissa-owned MVReg rows.
pub struct MvRegStoreCodec<V, A>(std::marker::PhantomData<(V, A)>);

impl<V, A> StoreRegisterCodec for MvRegStoreCodec<V, A>
where
    V: StoreValueCodec,
    A: StoreActorCodec,
{
    type Reg = MvReg<V, A>;

    fn encode_store_reg(reg: &Self::Reg) -> crate::Result<Vec<u8>> {
        encode_mvreg_row(reg)
    }

    fn decode_store_reg(bytes: &[u8]) -> crate::Result<Self::Reg> {
        decode_mvreg_row(bytes)
    }
}

/// Encodes one Mantissa-owned MVReg into the generic Cap'n Proto store row.
pub fn encode_mvreg_row<V, A>(reg: &MvReg<V, A>) -> crate::Result<Vec<u8>>
where
    V: StoreValueCodec,
    A: StoreActorCodec,
{
    let mut message = capnp::message::Builder::new_default();
    {
        let mut row = message.init_root::<store_capnp::mv_reg_row::Builder>();
        let mut entries = row.reborrow().init_entries(reg.entries().len() as u32);

        for (entry_index, entry) in reg.entries().iter().enumerate() {
            let mut entry_builder = entries.reborrow().get(entry_index as u32);
            let mut clock_builder = entry_builder
                .reborrow()
                .init_clock(entry.clock().len() as u32);

            for (clock_index, (actor, counter)) in entry.clock().iter().enumerate() {
                let mut clock_entry = clock_builder.reborrow().get(clock_index as u32);
                clock_entry.set_actor(&actor.encode_store_actor());
                clock_entry.set_counter(counter);
            }

            let value = entry.value().encode_store_value()?;
            entry_builder.set_value(&value);
        }
    }

    Ok(capnp::serialize::write_message_to_words(&message))
}

/// Decodes one Mantissa-owned MVReg from the generic Cap'n Proto store row.
pub fn decode_mvreg_row<V, A>(bytes: &[u8]) -> crate::Result<MvReg<V, A>>
where
    V: StoreValueCodec,
    A: StoreActorCodec,
{
    let mut cursor = Cursor::new(bytes);
    let reader = capnp::serialize::read_message(&mut cursor, capnp::message::ReaderOptions::new())
        .map_err(capnp_store_error)?;
    let row = reader
        .get_root::<store_capnp::mv_reg_row::Reader>()
        .map_err(capnp_store_error)?;
    let entries = row.get_entries().map_err(capnp_store_error)?;
    let mut decoded_entries = Vec::with_capacity(entries.len() as usize);

    for entry in entries.iter() {
        let clock_entries = entry.get_clock().map_err(capnp_store_error)?;
        let mut clock = VectorClock::new();
        for clock_entry in clock_entries.iter() {
            let actor = A::decode_store_actor(clock_entry.get_actor().map_err(capnp_store_error)?)?;
            clock.apply(actor, clock_entry.get_counter());
        }

        let value = V::decode_store_value(entry.get_value().map_err(capnp_store_error)?)?;
        decoded_entries.push(MvRegEntry::new(clock, value));
    }

    Ok(MvReg::from_entries(decoded_entries))
}

/// Converts Cap'n Proto read errors into the store crate error type.
fn capnp_store_error(error: capnp::Error) -> Error {
    Error::Other(format!("capnp store payload error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{MvRegStoreCodec, StoreRegisterCodec};
    use crate::mvreg::{MvReg, MvRegEntry, VectorClock};
    use uuid::Uuid;

    /// Empty MVReg rows must round-trip through the Cap'n Proto register codec.
    #[test]
    fn capnp_mvreg_row_roundtrips_empty_registers() {
        let reg = MvReg::<Vec<u8>, Uuid>::new();

        let encoded = super::encode_mvreg_row(&reg).expect("encode empty mvreg");
        let decoded =
            super::decode_mvreg_row::<Vec<u8>, Uuid>(&encoded).expect("decode empty mvreg");

        assert_eq!(decoded, reg);
    }

    /// Concurrent MVReg entries must preserve value bytes and vector clocks.
    #[test]
    fn capnp_mvreg_row_roundtrips_concurrent_registers() {
        let actor_a = Uuid::from_u128(1);
        let actor_b = Uuid::from_u128(2);
        let mut left_clock = VectorClock::new();
        left_clock.apply(actor_a, 1);
        let mut right_clock = VectorClock::new();
        right_clock.apply(actor_b, 7);
        let reg = MvReg::from_entries(vec![
            MvRegEntry::new(left_clock, b"left".to_vec()),
            MvRegEntry::new(right_clock, b"right".to_vec()),
        ]);

        let encoded = super::encode_mvreg_row(&reg).expect("encode concurrent mvreg");
        let decoded =
            super::decode_mvreg_row::<Vec<u8>, Uuid>(&encoded).expect("decode concurrent mvreg");

        assert_eq!(decoded, reg);
    }

    /// The register-codec trait keeps MVReg as one codec implementation, not a store requirement.
    #[test]
    fn capnp_mvreg_store_codec_roundtrips_through_register_trait() {
        let actor = Uuid::from_u128(10);
        let mut reg = MvReg::new();
        reg.write(actor, b"value".to_vec());

        let encoded = MvRegStoreCodec::<Vec<u8>, Uuid>::encode_store_reg(&reg)
            .expect("encode through register trait");
        let decoded = MvRegStoreCodec::<Vec<u8>, Uuid>::decode_store_reg(&encoded)
            .expect("decode through register trait");

        assert_eq!(decoded, reg);
    }

    /// Actor bytes must be validated before constructing the register clock.
    #[test]
    fn capnp_mvreg_row_rejects_invalid_actor_bytes() {
        let mut message = capnp::message::Builder::new_default();
        {
            let mut row = message.init_root::<crate::store_capnp::mv_reg_row::Builder>();
            let mut entries = row.reborrow().init_entries(1);
            let mut entry = entries.reborrow().get(0);
            entry.set_value(b"value");
            let mut clock = entry.reborrow().init_clock(1);
            let mut clock_entry = clock.reborrow().get(0);
            clock_entry.set_actor(&[1, 2, 3]);
            clock_entry.set_counter(1);
        }
        let encoded = capnp::serialize::write_message_to_words(&message);

        let error = super::decode_mvreg_row::<Vec<u8>, Uuid>(&encoded)
            .expect_err("invalid actor bytes should be rejected");

        assert!(
            error.to_string().contains("expected 16-byte UUID actor"),
            "unexpected error: {error}"
        );
    }
}
