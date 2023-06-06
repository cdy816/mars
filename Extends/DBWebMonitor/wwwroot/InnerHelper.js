export class WindowHelper {
    static DownloadFromFile(file, url) {
        const anchorElement = document.createElement('a');
        anchorElement.href = url;
        anchorElement.download = file ?? '';
        anchorElement.click();
        anchorElement.remove();
    }
    static async DownloadFromStream(fileName, contentStreamReference) {
        const arrayBuffer = await contentStreamReference.arrayBuffer();
        const blob = new Blob([arrayBuffer]);
        const url = URL.createObjectURL(blob);
        const anchorElement = document.createElement('a');
        anchorElement.href = url;
        anchorElement.download = fileName ?? '';
        anchorElement.click();
        anchorElement.remove();
        URL.revokeObjectURL(url);
    }
    static Alert(msg) {
        window.alert(msg);
    }
    static GetTextBoxValue(target) {
        return target.value;
    }
    static SetTextBoxValue(target, val) {
        target.value = val;
    }
    static AppendTextAreaValue(target, val) {
        target.append(val);
        target.scrollTop = target.scrollHeight;
    }
    static ClearTextAreaValue(target) {
        target.innerText = "";
    }
    static Dialog(id, options) {
        const myModal = new bootstrap.Modal(document.getElementById(id), options);
        myModal.show();
    }
}
