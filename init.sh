ssh-add - <<< "${SYNCBOT_SSH_KEY}"

python3 initialize_key_files.py
python3 sync_strategies.py

#!/bin/bash
tmux new-session -d -s hummingbot -n hbstrat
tmux send-keys -t hummingbot:hbstrat "python3 bin/hummingbot_quickstart.py" Enter

sleep 5

tmux send-keys -t hummingbot:hbstrat "import ${CONFIG_FILE_NAME}" Enter

#GET PID by parsing?? or Force PID
#TAIL PID
tail -f /dev/null

###### To attach to Hummingbot Session run
##   tmux attach -t hummingbot:hbstrat


