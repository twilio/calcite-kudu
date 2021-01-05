#!/bin/sh

# Decrypt the file
mkdir $HOME/secrets
# --batch to prevent interactive command
# --yes to assume "yes" for questions
gpg --quiet --batch --yes --decrypt  --passphrase=$TARBALL_PASSWORD \
    --output travis/local.secrets.tar \
    travis/actions-secrets.tar.gpg
