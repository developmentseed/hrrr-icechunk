uv sync --all-groups
nodeenv -m .venv -p --node=22.19.0
hash -r
npm install aws-cdk
npx cdk deploy
