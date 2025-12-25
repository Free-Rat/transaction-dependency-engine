{
  description = "Dev shell for TDE";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };
      in {
        devShells = {
          default = pkgs.mkShell {
            name = "tde-shell";

            # narzędzia dostępne w shellu
            buildInputs = with pkgs; [
              docker          # klient docker (daemon musi być włączony systemowo)
              # docker-compose package attribute contains a hyphen so access via string
              (pkgs."docker-compose")
              curl
              git
              bashInteractive
              rustc
              cargo
            ];

            shellHook = ''
                docker run --name=riak -d -p 8087:8087 -p 8098:8098 basho/riak-kv
            '';
          };
        };
      });
}
