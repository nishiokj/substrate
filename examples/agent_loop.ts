import Anthropic from "@anthropic-ai/sdk";
import { ExecutionerEnvironment, toolSchemas, type ToolSchema } from "@substrate/sdk";

const client = new Anthropic();
const model = process.env.ANTHROPIC_MODEL;

if (!model) {
  throw new Error("Set ANTHROPIC_MODEL before running this example.");
}

const env = await ExecutionerEnvironment.create({
  workspace: { kind: "new" },
  policy: { process: { allowExec: true, allowedCommands: ["python", "pytest"] } },
});
const session = await env.createSession();

function anthropicTools(schemas: ToolSchema[]) {
  return schemas.map((schema) => ({
    name: schema.name,
    description: schema.description,
    input_schema: schema.inputSchema,
  }));
}

const messages = [{
  role: "user" as const,
  content: "Create notes.txt with a short hello, then read it back.",
}];

try {
  const response = await client.messages.create({
    model,
    max_tokens: 1024,
    tools: anthropicTools(toolSchemas()),
    messages,
  });

  messages.push({ role: "assistant", content: response.content });

  const toolResults = [];
  for (const block of response.content) {
    if (block.type !== "tool_use") {
      continue;
    }

    const result = await session.execute({
      id: block.id,
      name: block.name,
      input: block.input,
    });
    toolResults.push({
      type: "tool_result" as const,
      tool_use_id: block.id,
      content: result.output,
    });
  }

  if (toolResults.length > 0) {
    messages.push({ role: "user", content: toolResults });
    const final = await client.messages.create({
      model,
      max_tokens: 1024,
      tools: anthropicTools(toolSchemas()),
      messages,
    });
    console.log(final.content[0].type === "text" ? final.content[0].text : final.content);
  }
} finally {
  await env.close();
}
