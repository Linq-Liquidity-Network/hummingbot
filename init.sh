#!/bin/bash
eval "$(ssh-agent -s)"

ssh-add - <<< "${SYNCBOT_SSH_KEY}"

ssh-keyscan bitbucket.org >> ~/.ssh/known_hosts

tmux new-session -d -s sync_strategies -n hbstrat
tmux send-keys -t sync_strategies:hbstrat "python3 /home/hummingbot/sync_strategies.py &" Enter

sleep 10

tmux new-session -d -s hummingbot -n hbstrat
tmux send-keys -t hummingbot:hbstrat "python3 /home/hummingbot/initialize_key_files.py" Enter
tmux send-keys -t hummingbot:hbstrat "python3 bin/hummingbot_quickstart.py" Enter

sleep 10

tmux send-keys -t hummingbot:hbstrat "import ${CONFIG_FILE_NAME}" Enter

tail -f /dev/null

###### To attach to Hummingbot Session run
##   tmux attach -t hummingbot:hbstrat
##   tmux attach -t sync_strategies:hbstrat
