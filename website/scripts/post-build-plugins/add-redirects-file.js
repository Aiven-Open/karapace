const path = require("path");
const fs = require("fs");

/**
 * function to add our redirects after build
 * @param {object} props - the `props` object returned from postBuild
 */
function addRedirectsFile(props) {
    const redirectFileName = "_redirects";
    const redirectFilePath = path.basename(redirectFileName);

    const destinationDirectory = props.outDir;

    if (!redirectFilePath) {
        console.error(`⚠️ Redirect file does not exist!`);
        return;
    }

    if (!fs.existsSync(destinationDirectory)) {
        console.error(
            `⚠️ Build directory "${destinationDirectory} does not exist.`
        );
        return;
    }

    // Construct the destination file path
    const destinationFilePath = path.join(
        destinationDirectory,
        redirectFileName
    );

    // Copy the file
    fs.copyFile(redirectFilePath, destinationFilePath, (err) => {
        if (err) {
            console.error(`Error copying file: ${err}`);
            return;
        }
        console.log(`_redirects copied to ${destinationDirectory}`);
    });
}

export { addRedirectsFile };
