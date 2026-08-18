{
  description = "Volumediator";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { nixpkgs, ... }:
    let
      forAllSystems = nixpkgs.lib.genAttrs [ "x86_64-linux" "aarch64-linux" ];
    in
    {
      packages = forAllSystems (system:
        let
          pkgs = import nixpkgs { inherit system; };
          volumediator = pkgs.buildGoModule rec {
            pname = "volumediator";
            version = "1.0.0"; # x-release-please-version
            src = pkgs.lib.cleanSourceWith {
              src = ./.;
              filter = path: type:
                pkgs.lib.cleanSourceFilter path type
                && builtins.baseNameOf path != "result";
            };
            vendorHash = "sha256-snbWSK4v5kuNpD1fRBby/MM87LjOOUbDGe9P9yfFxLg=";
            subPackages = [ "cmd/volumediator" ];
            env.CGO_ENABLED = "0";
            ldflags = [ "-s" "-w" "-X main.version=${version}" ];
          };
        in
        {
          inherit volumediator;
          default = volumediator;
          image = pkgs.dockerTools.buildLayeredImage {
            name = "volumediator";
            tag = "dev";
            contents = [ volumediator pkgs.e2fsprogs pkgs.util-linux pkgs.cacert ];
            config = {
              Entrypoint = [ "${volumediator}/bin/volumediator" ];
              User = "65532:65532";
            };
          };
        });
    };
}
