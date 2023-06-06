declare var bootstrap: any;

export class WindowHelper {

    public static DownloadFromFile(file: string, url: string) {
        const anchorElement = document.createElement('a');
        anchorElement.href = url;
        anchorElement.download = file ?? '';
        anchorElement.click();
        anchorElement.remove();
    }

    public static async DownloadFromStream(fileName: string, contentStreamReference: any) {
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

    public static Alert(msg: string) {
        window.alert(msg);
    }

    public static GetTextBoxValue(target: HTMLInputElement): string {
        return target.value;
    }

    public static SetTextBoxValue(target: HTMLInputElement, val: string) {
        target.value = val;
    }

    public static AppendTextAreaValue(target: HTMLAreaElement, val: string) {
        target.append(val);
    }

    public static ClearTextAreaValue(target: HTMLAreaElement) {
        target.innerText = "";
    }

    public static Dialog(id: string, options: any) {
        const myModal = new bootstrap.Modal(document.getElementById(id), options);
        myModal.show();
    }

}