import { Executioner } from "@executioner/sdk";

const env = await Executioner.create({
  workspace: "new",
  allowCommands: ["ls"],
});

try {
  await env.write("notes.txt", "hello");
  console.log(await env.read("notes.txt"));
  console.log(await env.bash("ls /workspace"));
} finally {
  await env.close();
}
