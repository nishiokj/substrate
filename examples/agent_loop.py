from __future__ import annotations

import os

from anthropic import Anthropic
from substrate import Executioner


client = Anthropic()
model = os.environ["ANTHROPIC_MODEL"]
messages = [{
    "role": "user",
    "content": "Create notes.txt with a short hello, then read it back.",
}]

with Executioner.create(workspace="new", allow_commands=["python", "pytest"]) as env:
    response = client.messages.create(
        model=model,
        max_tokens=1024,
        tools=env.tool_schemas(),
        messages=messages,
    )

    messages.append({"role": "assistant", "content": response.content})

    tool_results = []
    for block in response.content:
        if block.type != "tool_use":
            continue

        result = env.execute({
            "id": block.id,
            "name": block.name,
            "input": block.input,
        })
        tool_results.append({
            "type": "tool_result",
            "tool_use_id": block.id,
            "content": result.output,
        })

    if tool_results:
        messages.append({"role": "user", "content": tool_results})
        final = client.messages.create(
            model=model,
            max_tokens=1024,
            tools=env.tool_schemas(),
            messages=messages,
        )
        print(final.content[0].text)
