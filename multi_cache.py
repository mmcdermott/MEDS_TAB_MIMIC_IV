import sys
import subprocess

def usage():
    print(f"Usage: {sys.argv[0]} <number_of_processes>")
    print("  <number_of_processes>: The number of subprocesses to launch")

def main():
    if len(sys.argv) != 2:
        usage()
        sys.exit(1)

    try:
        num_processes = int(sys.argv[1])
        if num_processes <= 0:
            raise ValueError
    except ValueError:
        print("Error: Please provide a positive integer for the number of processes.")
        usage()
        sys.exit(1)

    print(f"Launching {num_processes} instances of 'bash -i test.sh'...")

    for i in range(1, num_processes + 1):
        print(f"Launching process {i}")
        subprocess.run(["bash", "-i", "test.sh"], check=True)

    print("All processes have completed.")

if __name__ == "__main__":
    main()