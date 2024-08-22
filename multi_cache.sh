#!/bin/bash

# Function to display usage information
usage() {
    echo "Usage: $0 <number_of_processes>"
    echo "  <number_of_processes>: The number of subprocesses to launch"
}

# Check if the number of processes is provided
if [ $# -ne 1 ]; then
    usage
    exit 1
fi

# Get the number of processes from the command line argument
num_processes=$1

# Check if the input is a positive integer
if ! [[ "$num_processes" =~ ^[1-9][0-9]*$ ]]; then
    echo "Error: Please provide a positive integer for the number of processes."
    usage
    exit 1
fi

echo "Launching $num_processes instances of 'bash -i test.sh'..."

# Launch the subprocesses
for (( i=1; i<=$num_processes; i++ ))
do
    echo "Launching process $i"
    bash -i test.sh &
done

# Wait for all subprocesses to finish
wait

echo "All processes have completed."