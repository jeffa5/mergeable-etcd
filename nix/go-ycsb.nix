{pkgs}:
pkgs.buildGoModule rec {
  pname = "go-ycsb";
  version = "1.0.0";

  src = pkgs.fetchFromGitHub {
    owner = "pingcap";
    repo = "go-ycsb";
    rev = "v${version}";
    sha256 = "sha256-bZnkyMw5p/klJ5bo9wtD97pACWGFYUN+tk506a/PClA=";
  };

  vendorSha256 = "sha256-2EW/MWgURAA7IvA96V7trLFuhTmefkk4q+kkwU5S/FE=";
}
