#!/bin/bash
ssh-add - <<< "${SYNCBOT_SSH_KEY}"

python3 initialize_key_files.py

tmux new-session -d -s sync_strategies -n hbstrat
tmux send-keys -t sync_strategies:hbstrat "python3 sync_strategies.py &" Enter

sleep 10

tmux new-session -d -s hummingbot -n hbstrat
tmux send-keys -t hummingbot:hbstrat "python3 bin/hummingbot_quickstart.py" Enter

sleep 5

tmux send-keys -t hummingbot:hbstrat "import ${CONFIG_FILE_NAME}" Enter

tail -f /dev/null

###### To attach to Hummingbot Session run
##   tmux attach -t hummingbot:hbstrat
