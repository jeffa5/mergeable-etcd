pub mod k8s {
    pub mod api {
        pub mod core {
            pub mod v1 {
                tonic::include_proto!("k8s.io.api.core.v1");
            }
        }

        pub mod coordination {
            pub mod v1 {
                tonic::include_proto!("k8s.io.api.coordination.v1");
            }
        }
    }

    pub mod apimachinery {
        pub mod pkg {
            pub mod api {
                pub mod resource {
                    tonic::include_proto!("k8s.io.apimachinery.pkg.api.resource");
                }
            }

            pub mod apis {
                pub mod meta {
                    pub mod v1 {
                        tonic::include_proto!("k8s.io.apimachinery.pkg.apis.meta.v1");
                    }
                }
            }

            pub mod runtime {
                tonic::include_proto!("k8s.io.apimachinery.pkg.runtime");

                pub mod schema {
                    tonic::include_proto!("k8s.io.apimachinery.pkg.runtime.schema");
                }
            }

            pub mod util {
                pub mod intstr {
                    tonic::include_proto!("k8s.io.apimachinery.pkg.util.intstr");
                }
            }
        }
    }
}
