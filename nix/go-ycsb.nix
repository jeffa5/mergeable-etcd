{
  fetchFromGitHub,
  buildGoModule,
  symlinkJoin,
  stdenv,
}: let
  version = "1.0.0";
  src = fetchFromGitHub {
    owner = "pingcap";
    repo = "go-ycsb";
    rev = "v${version}";
    sha256 = "sha256-bZnkyMw5p/klJ5bo9wtD97pACWGFYUN+tk506a/PClA=";
  };
  ycsb = buildGoModule rec {
    pname = "go-ycsb";
    inherit version src;

    vendorSha256 = "sha256-2EW/MWgURAA7IvA96V7trLFuhTmefkk4q+kkwU5S/FE=";
  };
  workloads = stdenv.mkDerivation {
    pname = "go-ycsb-workloads";
    inherit version src;
    dontBuild = true;
    installPhase = ''
      mkdir -p $out/lib
      cp -r workloads $out/lib/.
    '';
  };
in
  symlinkJoin {
    name = "ycsb";
    paths = [ycsb workloads];
  }
