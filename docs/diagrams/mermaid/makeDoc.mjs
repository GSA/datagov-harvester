import { run } from "@mermaid-js/mermaid-cli"
import { readdir } from 'node:fs/promises';
import { resolve } from 'node:path';

(async (req, res) => {
    try {
        let fileSrc = resolve('./docs/diagrams/mermaid/src')
        let fileDest = resolve('./docs/diagrams/mermaid/dest')
        const files = await readdir(fileSrc);
        for (const file of files) {
            console.log(`Found file: ${file}`);
            await run(
            `${fileSrc}/${file}`, `${fileDest}/${file}`, {puppeteerConfig: {"headless": "old"}},
            )
        }
    } catch (err) {
        console.error(err)
    }
})();
