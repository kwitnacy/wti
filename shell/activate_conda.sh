aconda(){
    # >>> conda initialize >>>
    # !! Contents within this block are managed by 'conda init' !!
    __conda_setup="$('/home/kwitnoncy/anaconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
    if [ $? -eq 0 ]; then
        eval "$__conda_setup"
    else
        if [ -f "/home/kwitnoncy/anaconda3/etc/profile.d/conda.sh" ]; then
 . "/home/kwitnoncy/anaconda3/etc/profile.d/conda.sh"  # commented out by conda initialize
        else
            export PATH="/home/kwitnoncy/anaconda3/bin:$PATH"  # commented out by conda initialize
        fi
    fi
    unset __conda_setup
    # <<< conda initialize <<<
    echo "conda is activated"
}
aconda
conda activate wti
