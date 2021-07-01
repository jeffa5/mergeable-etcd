use std::{
    collections::BTreeMap,
    env,
    fs::{read_dir, File},
    io::Write,
    path::PathBuf,
};

/// Find all generated.proto files in the proto directory recursively.
fn find_generated_protos() -> Vec<PathBuf> {
    let mut protos = Vec::new();
    let mut dirs = vec![
        PathBuf::from("proto/k8s.io/api"),
        PathBuf::from("proto/k8s.io/apimachinery"),
    ];
    while let Some(dir) = dirs.pop() {
        for entry in read_dir(&dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.is_file() {
                if let Some("proto") = path.extension().and_then(|s| s.to_str()) {
                    protos.push(path)
                }
            }
        }
    }
    protos
}

/// A tree of modules for exposing the proto modules.
#[derive(Debug, Default)]
struct ModuleTree {
    has_module: bool,
    child_modules: BTreeMap<String, ModuleTree>,
}

impl ModuleTree {
    fn insert(&mut self, mut path: Vec<String>) {
        if let Some(name) = path.pop() {
            let entry = self.child_modules.entry(name).or_default();
            entry.insert(path)
        } else {
            self.has_module = true
        }
    }

    fn render(self, path: Vec<String>) -> String {
        let indent = "    ".repeat(path.len());
        let mut output = String::new();
        if self.has_module {
            output.push_str(&format!(
                "{}tonic::include_proto!(\"k8s.io.{}\");\n",
                indent,
                path.join(".")
            ));
        }
        for (k, v) in self.child_modules {
            let start = format!("{}pub mod {} {{", indent, k);
            let end = format!("{}}}", indent);
            let mut p = path.clone();
            p.push(k);
            let middle = v.render(p);
            output.push_str(&format!("{}\n{}{}\n", start, middle, end))
        }
        output
    }
}

fn main() {
    let protos = find_generated_protos();
    let includes = [PathBuf::from("proto")];

    tonic_build::configure()
        .type_attribute(
            ".",
            "#[derive(serde::Serialize, serde::Deserialize, automergeable::Automergeable, arbitrary::Arbitrary)]",
        )
        .compile(
            &protos,
            &includes,
        )
        .expect("failed to compile protos");

    let proto_dirs = protos
        .iter()
        .map(|p| p.parent().unwrap().strip_prefix("proto/k8s.io/").unwrap());

    let mut module_tree = ModuleTree::default();
    for dir in proto_dirs {
        module_tree.insert(
            dir.iter()
                .map(|p| p.to_str().unwrap().to_owned())
                .rev()
                .collect(),
        );
    }

    let modules = module_tree.render(Vec::new());

    let out_dir = env::var("OUT_DIR").unwrap();
    let out_src_lib = format!("{}/all.rs", out_dir);

    let mut f = File::create(out_src_lib).unwrap();
    write!(f, "{}", modules).unwrap();
}
