{ pkgs ? import <nixpkgs> {} }:
# This is totally independent of the devenv stuff, using crate2nix directly instead.
# There is probably a much better way to do this.
let cargo_nix = pkgs.callPackage ./Cargo.nix {};
in cargo_nix.rootCrate.build
