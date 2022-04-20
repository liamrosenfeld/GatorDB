from parse import parse_line, initialize_db


def run_interactive(args):
    print("Welcome to GatorDB!")
    if args.dbpath:
        initialize_db(args.dbpath)
    while True:
        line = input("$ ")
        if line.lower() in ("exit", "quit"):
            break
        parse_line(line)
