from substrate import ExecutionerEnvironment

with ExecutionerEnvironment.create(
    workspace={"kind": "new"},
    policy={"process": {"allowExec": True, "allowedCommands": ["ls"]}},
) as env:
    session = env.create_session()
    session.write("notes.txt", "hello")
    print(session.read("notes.txt"))
    print(session.bash("ls /workspace"))
