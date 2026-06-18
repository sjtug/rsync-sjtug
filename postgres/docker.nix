{
  craneLib,
  lib,
  nix2container,
  pkgs,
}:
let
  postgresql = pkgs.postgresql_15.withPackages (ps: [ ps.pg_cron ]);

  # Nest all layers so that prior layers are dependencies of later layers.
  # This way, we should avoid redundant dependencies.
  foldImageLayers =
    let
      mergeToLayer =
        priorLayers: component:
        assert builtins.isList priorLayers;
        assert builtins.isAttrs component;
        let
          layer = nix2container.buildLayer (
            component
            // {
              layers = priorLayers;
            }
          );
        in
        priorLayers ++ [ layer ];
    in
    layers: lib.foldl mergeToLayer [ ] layers;

  migrationSrc = lib.fileset.toSource {
    root = ./oneshot-migration;
    fileset = craneLib.fileset.commonCargoSources ./oneshot-migration;
  };

  oneshot-migration = craneLib.buildPackage {
    src = migrationSrc;
    strictDeps = true;
    preBuild = ''
      ln -sfn ${./migrations} ../migrations
    '';
  };

  rootDir = pkgs.runCommand "postgres-rsync-sjtug-root" { } ''
    mkdir -p \
      $out/bin \
      $out/etc/postgresql \
      $out/docker-entrypoint-initdb.d \
      $out/tmp \
      $out/usr/bin \
      $out/var/lib/postgresql/data \
      $out/var/run/postgresql

    cp ${./postgresql.conf} $out/etc/postgresql/postgresql.conf
    cp ${./setup.sh} $out/docker-entrypoint-initdb.d/setup.sh
    ln -s ${pkgs.bash}/bin/bash $out/bin/bash
    ln -s ${pkgs.bash}/bin/sh $out/bin/sh
    ln -s ${pkgs.coreutils}/bin/env $out/usr/bin/env

    cat > $out/etc/passwd <<'EOF'
    root:x:0:0:root:/root:/bin/sh
    postgres:x:999:999:PostgreSQL Server:/var/lib/postgresql:/bin/sh
    EOF

    cat > $out/etc/group <<'EOF'
    root:x:0:
    postgres:x:999:
    EOF
  '';

  entrypoint = pkgs.writeShellApplication {
    name = "postgres-rsync-sjtug-entrypoint";
    runtimeInputs = [
      pkgs.bash
      pkgs.coreutils
      pkgs.su-exec
      postgresql
      oneshot-migration
    ];
    text = ''
      set -euo pipefail

      : "''${POSTGRES_USER:=rsync-sjtug}"
      : "''${POSTGRES_DB:=$POSTGRES_USER}"
      : "''${PGDATA:=/var/lib/postgresql/data}"
      : "''${PGHOST:=/var/run/postgresql}"

      export POSTGRES_USER POSTGRES_DB PGDATA PGHOST

      if [ "$(id -u)" = "0" ]; then
        mkdir -p "$PGDATA" "$PGHOST"
        chmod 700 "$PGDATA"
        chmod 3775 "$PGHOST"
        chown -R postgres:postgres "$PGDATA" "$PGHOST"
        exec su-exec postgres:postgres "$0" "$@"
      fi

      if [ ! -s "$PGDATA/PG_VERSION" ]; then
        initdb -D "$PGDATA" --username="$POSTGRES_USER"
        cat >> "$PGDATA/pg_hba.conf" <<'EOF'
      host    all             all             0.0.0.0/0               trust
      host    all             all             ::/0                    trust
      EOF

        pg_ctl -D "$PGDATA" \
          -o "-c listen_addresses= -c unix_socket_directories=$PGHOST" \
          -w start

        createdb --username="$POSTGRES_USER" "$POSTGRES_DB"

        pg_ctl -D "$PGDATA" -m fast -w stop

        pg_ctl -D "$PGDATA" \
          -o "-c config_file=/etc/postgresql/postgresql.conf -c listen_addresses= -c unix_socket_directories=$PGHOST" \
          -w start

        bash /docker-entrypoint-initdb.d/setup.sh

        pg_ctl -D "$PGDATA" -m fast -w stop
      fi

      exec postgres -D "$PGDATA" "$@"
    '';
  };
in
nix2container.buildImage {
  name = "photonquantum/postgres-rsync-sjtug";
  tag = "latest";

  copyToRoot = [
    rootDir
  ];

  layers =
    let
      layerDefs = [
        { deps = [ oneshot-migration ]; }
        { deps = [ postgresql ]; }
        {
          deps = [
            pkgs.bash
            pkgs.coreutils
            pkgs.su-exec
          ];
        }
        { deps = [ entrypoint ]; }
      ];
    in
    foldImageLayers layerDefs;

  perms = [
    {
      path = rootDir;
      regex = "tmp";
      mode = "1777";
    }
    {
      path = rootDir;
      regex = "var/lib/postgresql/data";
      mode = "0700";
      uid = 999;
      gid = 999;
    }
    {
      path = rootDir;
      regex = "var/run/postgresql";
      mode = "3775";
      uid = 999;
      gid = 999;
    }
  ];

  config = {
    Cmd = [
      "-c"
      "config_file=/etc/postgresql/postgresql.conf"
      "-c"
      "unix_socket_directories=/var/run/postgresql"
    ];
    Entrypoint = [ "${entrypoint}/bin/postgres-rsync-sjtug-entrypoint" ];
    Env = [
      "POSTGRES_USER=rsync-sjtug"
      "POSTGRES_DB=rsync-sjtug"
      "PGDATA=/var/lib/postgresql/data"
      "PGHOST=/var/run/postgresql"
      "PATH=${
        lib.makeBinPath [
          postgresql
          oneshot-migration
          pkgs.bash
          pkgs.coreutils
          pkgs.su-exec
        ]
      }"
    ];
    ExposedPorts."5432/tcp" = { };
    User = "0";
    Volumes."/var/lib/postgresql/data" = { };
    WorkingDir = "/";
  };
}
