{
  description = "AWS CloudWatch CLI";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
  };

  outputs = { self, nixpkgs }: let
    allSystems = [
      "aarch64-linux" # 64-bit ARM Linux
      "x86_64-darwin" # 64-bit Intel macOS
      "aarch64-darwin" # 64-bit ARM macOS
    ];
    system = builtins.elemAt allSystems 2;
    pkgs = import nixpkgs { inherit system; };
    lib = nixpkgs.lib;
  in {
    clougowatch=pkgs.buildGoModule{
      pname="clougowatch";
      version="0.0.1";
      nativeBuildInputs= [pkgs.go_1_23];
      src=./.;
      proxyVendor = true;
      preBuild="go mod tidy";
      # How to update
      # 1. build with fakeHash
      # vendorHash=lib.fakeHash;
      # 2. use the hash via stderr
      vendorHash="sha256-Mj8ZbRG9ASKzPF9Vnwro/08oAoNIVT2rOQXKwIeVmXM=";
    };
    devShells.${system}.default = pkgs.mkShell {
      packages = [pkgs.sqlite pkgs.go_1_23, pkgs.python312];
    };
  };
}
