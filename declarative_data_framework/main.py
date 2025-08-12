import sys
from src.cli import run, create, update, test

def main():
    if len(sys.argv) < 3:
        print("Usage: python main.py <command> <path_to_yaml>")
        print("Available commands: run, create, update, test")
        sys.exit(1)

    command = sys.argv[1]
    config_path = sys.argv[2]

    print(f"Loading and validating configuration from: {config_path}")

    if command == 'run':
        run(config_path)
    elif command == 'create':
        create(config_path)
    elif command == 'update':
        update(config_path)
    elif command == 'test':
        test(config_path)
    else:
        print(f"Command '{command}' not recognized.")

if __name__ == "__main__":
    main()
