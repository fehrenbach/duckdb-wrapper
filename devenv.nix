{ pkgs, lib, config, ... }: {
  packages = [
    pkgs.duckdb
  ];

  languages.rust = {
    enable = true;
    components = [ "rustc" "cargo" "clippy" "rustfmt" "rust-analyzer" ];
  };

  # See full reference at https://devenv.sh/reference/options/
}
