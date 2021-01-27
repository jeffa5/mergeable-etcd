#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UserAddOptions {
    #[prost(bool, tag = "1")]
    pub no_password: bool,
}
/// User is a single entry in the bucket authUsers
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct User {
    #[prost(bytes = "vec", tag = "1")]
    pub name: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub password: ::prost::alloc::vec::Vec<u8>,
    #[prost(string, repeated, tag = "3")]
    pub roles: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, optional, tag = "4")]
    pub options: ::core::option::Option<UserAddOptions>,
}
/// Permission is a single entity
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Permission {
    #[prost(enumeration = "permission::Type", tag = "1")]
    pub perm_type: i32,
    #[prost(bytes = "vec", tag = "2")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub range_end: ::prost::alloc::vec::Vec<u8>,
}
/// Nested message and enum types in `Permission`.
pub mod permission {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Type {
        Read = 0,
        Write = 1,
        Readwrite = 2,
    }
}
/// Role is a single entry in the bucket authRoles
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Role {
    #[prost(bytes = "vec", tag = "1")]
    pub name: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "2")]
    pub key_permission: ::prost::alloc::vec::Vec<Permission>,
}
