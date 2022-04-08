from parse import parse_line


def run_interactive():
    print("Welcome to GatorDB!")
    while True:
        line = input("$ ")
        if line.lower() in ("exit", "quit"):
            break
        parse_line(line)
