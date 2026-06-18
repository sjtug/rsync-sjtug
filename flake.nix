{
  description = "A Nix-flake-based Rust development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane"; # Check https://crane.dev for detailed guide
    advisory-db = {
      url = "github:rustsec/advisory-db";
      flake = false;
    };
    pre-commit-hooks = {
      url = "github:cachix/git-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{ flake-parts, advisory-db, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        inputs.treefmt-nix.flakeModule
        inputs.pre-commit-hooks.flakeModule
      ];

      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];

      perSystem =
        {
          config,
          pkgs,
          lib,
          system,
          ...
        }:
        let
          inherit (pkgs)
            stdenv
            pkgsStatic
            cacert
            rust-jemalloc-sys
            ;
          craneLib = (inputs.crane.mkLib pkgs).overrideToolchain (p: p.rustToolchain);
          src = lib.fileset.toSource {
            root = ./.;
            fileset = lib.fileset.unions [
              (craneLib.fileset.commonCargoSources ./.)
              ./.sqlx
              ./rsync-gateway/locales
              ./rsync-gateway/templates
              ./sqls
              ./tests/migrations
            ];
          };

          # NOTE: `buildInputs` and sometimes `nativeBuildInputs`
          # should be explicitly overridden for cross compilation
          commonArgs = {
            inherit src;
            strictDeps = true;
            nativeBuildInputs = [ cacert ];
            buildInputs = [ rust-jemalloc-sys ];
            doCheck = false; # Test separately with cargo-nextest
          };

          # Build *just* the cargo dependencies (of the entire workspace),
          # so we can reuse all of that work (e.g. via cachix) when running in CI
          # It is *highly* recommended to use something like cargo-hakari to avoid
          # cache misses when building individual top-level-crates
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;

          individualCrateArgs = commonArgs // {
            inherit cargoArtifacts;
            inherit (craneLib.crateNameFromCargoToml { inherit src; }) version;
            # NB: we disable tests since we'll run them all via cargo-nextest
            doCheck = false;
          };

          fileSetForCrate =
            crate:
            lib.fileset.toSource {
              root = ./.;
              fileset = lib.fileset.unions (
                [
                  ./Cargo.toml
                  ./Cargo.lock
                  ./.sqlx
                  ./sqls
                  ./tests/migrations
                  (craneLib.fileset.commonCargoSources ./rsync-core)
                  (craneLib.fileset.commonCargoSources crate)
                ]
                ++ lib.optionals (crate == ./rsync-gateway) [
                  ./rsync-gateway/locales
                  ./rsync-gateway/templates
                ]
              );
            };

          defaultTarget = stdenv.hostPlatform.config;
          muslTarget =
            {
              "x86_64-linux" = "x86_64-unknown-linux-musl";
              "aarch64-linux" = "aarch64-unknown-linux-musl";
              "aarch64-darwin" = throw "${system} does not support cross compilation to musl";
            }
            .${system};

          # Build the top-level crates of the workspace as individual derivations.
          # This allows consumers to only depend on (and build) only what they need.
          # Though it is possible to build the entire workspace as a single derivation,
          # so this is left up to you on how to organize things
          #
          # Note that the cargo workspace must define `workspace.members` using wildcards,
          # otherwise, omitting a crate (like we do below) will result in errors since
          # cargo won't be able to find the sources for all members.
          rsync-core = craneLib.buildPackage (
            individualCrateArgs
            // {
              pname = "rsync-core";
              cargoExtraArgs = "-p rsync-core";
              src = fileSetForCrate ./rsync-core;
              CARGO_PROFILE = "dev";
              CARGO_BUILD_TARGET = defaultTarget;
            }
          );
          rsync-fetcher = craneLib.buildPackage (
            individualCrateArgs
            // {
              pname = "rsync-fetcher";
              cargoExtraArgs = "-p rsync-fetcher";
              src = fileSetForCrate ./rsync-fetcher;
              CARGO_PROFILE = "dev";
              CARGO_BUILD_TARGET = defaultTarget;
            }
          );
          rsync-gateway = craneLib.buildPackage (
            individualCrateArgs
            // {
              pname = "rsync-gateway";
              cargoExtraArgs = "-p rsync-gateway";
              src = fileSetForCrate ./rsync-gateway;
              CARGO_PROFILE = "dev";
              CARGO_BUILD_TARGET = defaultTarget;
            }
          );
          rsync-gc = craneLib.buildPackage (
            individualCrateArgs
            // {
              pname = "rsync-gc";
              cargoExtraArgs = "-p rsync-gc";
              src = fileSetForCrate ./rsync-gc;
              CARGO_PROFILE = "dev";
              CARGO_BUILD_TARGET = defaultTarget;
            }
          );

          # my-workspace-musl = craneLib.buildPackage (
          #   commonArgs
          #   // {
          #     nativeBuildInputs = [
          #       # Required by aws-lc-sys
          #       cacert
          #       stdenv.cc
          #       pkgsStatic.stdenv.cc
          #     ];
          #     buildInputs = [ pkgsStatic.rust-jemalloc-sys ];
          #     doCheck = true; # always run checkPhase for release artifact
          #     CARGO_PROFILE = "release";
          #     CARGO_BUILD_TARGET = muslTarget;
          #     CARGO_BUILD_FLAGS = "-C target-feature=+crt-static";
          #   }
          # );
        in
        {
          _module.args.pkgs = import inputs.nixpkgs {
            inherit system;
            overlays = [
              inputs.rust-overlay.overlays.default
              (_final: prev: {
                rustToolchain =
                  let
                    rust = prev.rust-bin;
                  in
                  rust.stable.latest.default.override {
                    extensions = [
                      "rust-src"
                      "rust-analyzer"
                    ];
                    targets = [ muslTarget ];
                  };
              })
            ];
          };

          # https://flake.parts/options/treefmt-nix.html
          # Example: https://github.com/nix-community/buildbot-nix/blob/main/nix/treefmt/flake-module.nix
          treefmt = {
            projectRootFile = "flake.nix";
            flakeCheck = false;
            settings.global.excludes = [ ];

            programs = {
              nixfmt.enable = true;
              taplo.enable = true;
              # zizmor.enable = true;
            };
          };

          # https://flake.parts/options/git-hooks-nix.html
          # Example: https://github.com/cachix/git-hooks.nix/blob/master/template/flake.nix
          pre-commit.settings.package = pkgs.prek;
          pre-commit.settings.configPath = ".pre-commit-config.flake.yaml";
          pre-commit.settings.hooks = {
            commitizen.enable = true;
            eclint.enable = true;
            treefmt.enable = true;
          };

          checks = {
            # Build the crate as part of `nix flake check` for convenience
            inherit
              rsync-core
              rsync-fetcher
              rsync-gateway
              rsync-gc
              ;

            # Run clippy (and deny all warnings) on the crate source,
            # again, reusing the dependency artifacts from above.
            #
            # Note that this is done as a separate derivation so that
            # we can block the CI if there are issues here, but not
            # prevent downstream consumers from building our crate by itself.
            my-workspace-clippy = craneLib.cargoClippy (
              commonArgs
              // {
                inherit cargoArtifacts;
                cargoClippyExtraArgs = "--all-targets -- --deny warnings";
              }
            );

            my-workspace-doc = craneLib.cargoDoc (
              commonArgs
              // {
                inherit cargoArtifacts;
                # This can be commented out or tweaked as necessary, e.g. set to
                # `--deny rustdoc::broken-intra-doc-links` to only enforce that lint
                env.RUSTDOCFLAGS = "--deny warnings";
              }
            );

            # Check Rust project formatting
            my-workspace-fmt = craneLib.cargoFmt {
              inherit src;
            };

            # Check TOML formatting
            my-workspace-toml-fmt = craneLib.taploFmt {
              src = pkgs.lib.sources.sourceFilesBySuffices src [ ".toml" ];
              # taplo arguments can be further customized below as needed
              # taploExtraArgs = "--config ./taplo.toml";
            };

            # Audit dependencies
            my-workspace-audit = craneLib.cargoAudit {
              inherit src advisory-db;
            };

            # Audit licenses
            my-workspace-deny = craneLib.cargoDeny {
              inherit src;
            };

            # Run tests with cargo-nextest
            my-workspace-nextest = craneLib.cargoNextest (
              commonArgs
              // {
                inherit cargoArtifacts;
                doCheck = true;
                nativeBuildInputs = commonArgs.nativeBuildInputs ++ [
                  pkgs.postgresql_16
                  pkgs.postgresqlTestHook
                ];
                postgresqlTestUserOptions = "LOGIN CREATEDB";
                postgresqlTestSetupPost = ''
                  export DATABASE_URL="postgresql:///$PGDATABASE?host=$PGHOST&user=$PGUSER"
                  export SQLX_OFFLINE=true
                '';
                cargoNextestExtraArgs = "-E '!test(/.*_prop/)'";
              }
            );

            # Ensure that cargo-hakari is up to date
            my-workspace-hakari = craneLib.mkCargoDerivation {
              inherit src;
              pname = "my-workspace-hakari";
              cargoArtifacts = null;
              doInstallCargoArtifacts = false;

              buildPhaseCargoCommand = ''
                cargo hakari generate --diff  # workspace-hack Cargo.toml is up-to-date
                cargo hakari manage-deps --dry-run  # all workspace crates depend on workspace-hack
                cargo hakari verify
              '';

              nativeBuildInputs = [
                pkgs.cargo-hakari
              ];
            };
          };

          devShells.default = craneLib.devShell {
            inputsFrom = [
              config.treefmt.build.devShell
              config.pre-commit.devShell
            ];

            # Required for tikv-jemallocator
            hardeningDisable = [ "fortify" ];

            packages =
              with pkgs;
              [
                # pkg-config
                # rustPlatform.bindgenHook
                cargo-hakari

                ### Miscellaneous ###
                # cargo-audit
                cargo-bloat
                # cargo-license
                # cargo-nextest
                # cargo-outdated
                # cargo-show-asm
                # samply
                # watchexec
                # bacon
              ]
              ++ lib.optionals (!pkgs.stdenv.isDarwin) [
                cargo-llvm-cov
                # valgrind
              ];
          };

          # packages.default = ;
        };
    };
}
