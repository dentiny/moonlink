# VSCode devcontainer config.
git config devcontainers-theme.show-dirty 1

# pg access.
echo "alias psql='PGPASSWORD=postgres psql -h postgres -p 5432 -U postgres -d postgres'" >> ~/.bashrc

# precommit hook requirement.
pre-commit install -t pre-push
