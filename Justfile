test-all:
    cargo nextest run --workspace -E '!test(/.*_prop/)'

test-prop TEST:
    #!/usr/bin/env bash
    if [ ! -f tests/vnu/vnu.jar ]; then
      TEMP_DIR=$(mktemp -d)
      wget -O $TEMP_DIR/vnu.jar.zip https://github.com/validator/validator/releases/download/20.6.30/vnu.jar_20.6.30.zip
      unzip $TEMP_DIR/vnu.jar.zip -d $TEMP_DIR
      cp $TEMP_DIR/dist/vnu.jar tests/vnu/vnu.jar
    fi
    java -Dnu.validator.servlet.bind-address=127.0.0.1 -cp tests/vnu/vnu.jar nu.validator.servlet.Main 8649 &
    VNU_PID=$!
    cargo nextest run --workspace -E 'test({{TEST}})'
    kill $VNU_PID

test-prop-all: (test-prop '/.*_prop/')