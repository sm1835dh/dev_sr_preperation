#\!/bin/bash
source .venv/bin/activate
export PATH="/opt/homebrew/opt/postgresql@14/bin:$PATH"
python "$@"
