use std::time::SystemTime;

pub const CFG_STD: bincode::config::Configuration = bincode::config::standard();
pub const CFG_STD_OLD: bincode_old::config::Configuration = bincode_old::config::standard();

#[derive(Debug, Clone)]
pub struct Metadata {
    pub len: u64,
    pub modify_time: SystemTime,
    pub extra: MetaExtra,
}

impl ::bincode::Encode for Metadata {
    fn encode<__E: ::bincode::enc::Encoder>(
        &self,
        encoder: &mut __E,
    ) -> core::result::Result<(), ::bincode::error::EncodeError> {
        ::bincode::Encode::encode(&self.len, encoder)?;
        ::bincode::Encode::encode(&self.modify_time, encoder)?;
        ::bincode::Encode::encode(&self.extra, encoder)?;
        Ok(())
    }
}

impl ::bincode::Decode for Metadata {
    fn decode<__D: ::bincode::de::Decoder>(
        decoder: &mut __D,
    ) -> core::result::Result<Self, ::bincode::error::DecodeError> {
        Ok(Self {
            len: ::bincode::Decode::decode(decoder)?,
            modify_time: ::bincode::Decode::decode(decoder)?,
            extra: ::bincode::Decode::decode(decoder)?,
        })
    }
}

impl<'__de> ::bincode::BorrowDecode<'__de> for Metadata {
    fn borrow_decode<__D: ::bincode::de::BorrowDecoder<'__de>>(
        decoder: &mut __D,
    ) -> core::result::Result<Self, ::bincode::error::DecodeError> {
        Ok(Self {
            len: ::bincode::BorrowDecode::borrow_decode(decoder)?,
            modify_time: ::bincode::BorrowDecode::borrow_decode(decoder)?,
            extra: ::bincode::BorrowDecode::borrow_decode(decoder)?,
        })
    }
}

impl ::bincode_old::Encode for Metadata {
    fn encode<__E: ::bincode_old::enc::Encoder>(
        &self,
        encoder: &mut __E,
    ) -> core::result::Result<(), ::bincode_old::error::EncodeError> {
        ::bincode_old::Encode::encode(&self.len, encoder)?;
        ::bincode_old::Encode::encode(&self.modify_time, encoder)?;
        ::bincode_old::Encode::encode(&self.extra, encoder)?;
        Ok(())
    }
}

impl ::bincode_old::Decode for Metadata {
    fn decode<__D: ::bincode_old::de::Decoder>(
        decoder: &mut __D,
    ) -> core::result::Result<Self, ::bincode_old::error::DecodeError> {
        Ok(Self {
            len: ::bincode_old::Decode::decode(decoder)?,
            modify_time: ::bincode_old::Decode::decode(decoder)?,
            extra: ::bincode_old::Decode::decode(decoder)?,
        })
    }
}

impl<'__de> ::bincode_old::BorrowDecode<'__de> for Metadata {
    fn borrow_decode<__D: ::bincode_old::de::BorrowDecoder<'__de>>(
        decoder: &mut __D,
    ) -> core::result::Result<Self, ::bincode_old::error::DecodeError> {
        Ok(Self {
            len: ::bincode_old::BorrowDecode::borrow_decode(decoder)?,
            modify_time: ::bincode_old::BorrowDecode::borrow_decode(decoder)?,
            extra: ::bincode_old::BorrowDecode::borrow_decode(decoder)?,
        })
    }
}

#[derive(Debug, Clone)]
pub enum MetaExtra {
    Symlink { target: Vec<u8> },
    Regular { blake2b_hash: [u8; 20] },
    Directory,
}

impl ::bincode::Encode for MetaExtra {
    fn encode<__E: ::bincode::enc::Encoder>(
        &self,
        encoder: &mut __E,
    ) -> core::result::Result<(), ::bincode::error::EncodeError> {
        match self {
            Self::Symlink { target } => {
                <u32 as ::bincode::Encode>::encode(&(0u32), encoder)?;
                ::bincode::Encode::encode(target, encoder)?;
                Ok(())
            }
            Self::Regular { blake2b_hash } => {
                <u32 as ::bincode::Encode>::encode(&(1u32), encoder)?;
                ::bincode::Encode::encode(blake2b_hash, encoder)?;
                Ok(())
            }
            Self::Directory => {
                <u32 as ::bincode::Encode>::encode(&(2u32), encoder)?;
                Ok(())
            }
        }
    }
}

impl ::bincode_old::Encode for MetaExtra {
    fn encode<__E: ::bincode_old::enc::Encoder>(
        &self,
        encoder: &mut __E,
    ) -> core::result::Result<(), ::bincode_old::error::EncodeError> {
        match self {
            Self::Symlink { target } => {
                <u32 as ::bincode_old::Encode>::encode(&(0u32), encoder)?;
                ::bincode_old::Encode::encode(target, encoder)?;
                Ok(())
            }
            Self::Regular { blake2b_hash } => {
                <u32 as ::bincode_old::Encode>::encode(&(1u32), encoder)?;
                ::bincode_old::Encode::encode(blake2b_hash, encoder)?;
                Ok(())
            }
            Self::Directory => {
                <u32 as ::bincode_old::Encode>::encode(&(2u32), encoder)?;
                Ok(())
            }
        }
    }
}

impl ::bincode::Decode for MetaExtra {
    fn decode<__D: ::bincode::de::Decoder>(
        decoder: &mut __D,
    ) -> core::result::Result<Self, ::bincode::error::DecodeError> {
        let variant_index = <u32 as ::bincode::Decode>::decode(decoder)?;
        match variant_index {
            0u32 => Ok(Self::Symlink {
                target: ::bincode::Decode::decode(decoder)?,
            }),
            1u32 => Ok(Self::Regular {
                blake2b_hash: ::bincode::Decode::decode(decoder)?,
            }),
            2u32 => Ok(Self::Directory {}),
            variant => Err(::bincode::error::DecodeError::UnexpectedVariant {
                found: variant,
                type_name: "MetaExtra",
                allowed: &::bincode::error::AllowedEnumVariants::Range { min: 0, max: 2 },
            }),
        }
    }
}

impl<'__de> ::bincode::BorrowDecode<'__de> for MetaExtra {
    fn borrow_decode<__D: ::bincode::de::BorrowDecoder<'__de>>(
        decoder: &mut __D,
    ) -> core::result::Result<Self, ::bincode::error::DecodeError> {
        let variant_index = <u32 as ::bincode::Decode>::decode(decoder)?;
        match variant_index {
            0u32 => Ok(Self::Symlink {
                target: ::bincode::BorrowDecode::borrow_decode(decoder)?,
            }),
            1u32 => Ok(Self::Regular {
                blake2b_hash: ::bincode::BorrowDecode::borrow_decode(decoder)?,
            }),
            2u32 => Ok(Self::Directory {}),
            variant => Err(::bincode::error::DecodeError::UnexpectedVariant {
                found: variant,
                type_name: "MetaExtra",
                allowed: &::bincode::error::AllowedEnumVariants::Range { min: 0, max: 2 },
            }),
        }
    }
}

impl ::bincode_old::Decode for MetaExtra {
    fn decode<__D: ::bincode_old::de::Decoder>(
        decoder: &mut __D,
    ) -> core::result::Result<Self, ::bincode_old::error::DecodeError> {
        let variant_index = <u32 as ::bincode_old::Decode>::decode(decoder)?;
        match variant_index {
            0u32 => Ok(Self::Symlink {
                target: ::bincode_old::Decode::decode(decoder)?,
            }),
            1u32 => Ok(Self::Regular {
                blake2b_hash: ::bincode_old::Decode::decode(decoder)?,
            }),
            2u32 => Ok(Self::Directory {}),
            variant => Err(::bincode_old::error::DecodeError::UnexpectedVariant {
                found: variant,
                type_name: "MetaExtra",
                allowed: &::bincode_old::error::AllowedEnumVariants::Range { min: 0, max: 2 },
            }),
        }
    }
}

impl<'__de> ::bincode_old::BorrowDecode<'__de> for MetaExtra {
    fn borrow_decode<__D: ::bincode_old::de::BorrowDecoder<'__de>>(
        decoder: &mut __D,
    ) -> core::result::Result<Self, ::bincode_old::error::DecodeError> {
        let variant_index = <u32 as ::bincode_old::Decode>::decode(decoder)?;
        match variant_index {
            0u32 => Ok(Self::Symlink {
                target: ::bincode_old::BorrowDecode::borrow_decode(decoder)?,
            }),
            1u32 => Ok(Self::Regular {
                blake2b_hash: ::bincode_old::BorrowDecode::borrow_decode(decoder)?,
            }),
            2u32 => Ok(Self::Directory {}),
            variant => Err(::bincode_old::error::DecodeError::UnexpectedVariant {
                found: variant,
                type_name: "MetaExtra",
                allowed: &::bincode_old::error::AllowedEnumVariants::Range { min: 0, max: 2 },
            }),
        }
    }
}

pub fn try_parse(data: &[u8]) -> Option<(Metadata, bool)> {
    // (parsed, is_new)
    let (new_parsed, remaining): (Metadata, _) = bincode::decode_from_slice(data, CFG_STD).ok()?;
    if remaining == data.len() {
        return Some((new_parsed, true));
    }
    let (old_parsed, remaining): (Metadata, _) =
        bincode_old::decode_from_slice(data, CFG_STD_OLD).ok()?;
    if remaining == data.len() {
        return Some((old_parsed, false));
    }
    None
}
