import sys
import os

from tkinter import Frame, Tk, BOTH, Button, Label, messagebox, scrolledtext, Entry, END
from tkinter import filedialog
from tkinter.ttk import Combobox

import requests
import pickle

from console_label import ConsoleLabel
from io import BytesIO


class ClientApp(Tk):

    def __init__(self):

        Tk.__init__(self)
        self._frame = None
        self.switch_frame(MainView)

    def switch_frame(self, frame_class):
        """Destroys current frame and replaces it with a new one."""
        new_frame = frame_class(self)

        if self._frame is not None:
            self._frame.destroy()

        self._frame = new_frame
        self._frame.pack()


class MainView(Frame):
    run_algorithm_url = 'http://localhost:5000/run_algorithm'

    def __init__(self, master):
        Frame.__init__(self, master)

        self.init_ui()

    def init_ui(self):

        self.master.title("Backbone")
        self.master.geometry("300x150")

        self.pack(fill=BOTH, expand=1)

        self.btn_upload_file = Button(self, text="Upload file", command=self.upload_file)
        self.btn_upload_file.place(x=90, y=10)

        self.btn_create_training_file = Button(self, text="Create & upload training file", command=self.create_training_file)
        self.btn_create_training_file.place(x=30, y=40)

        self.btn_run_algorithm = Button(self, text="Run algorithm", command=self.run_algorithm)
        self.btn_run_algorithm.place(x=80, y=70)

        self.btn_view_results = Button(self, text="View Results", command=self.view_results)
        self.btn_view_results.place(x=85, y=100)

    def upload_file(self):
        self.master.switch_frame(UploadFileView)

    def create_training_file(self):
        self.master.switch_frame(TrainingFileView)

    def run_algorithm(self):
        messagebox.showinfo("Information",
                            "Be patient! It will take a while... (Press OK to really start the algorithm :)")

        r = requests.post(self.run_algorithm_url)

        if r.status_code == requests.codes.ok:
            messagebox.showinfo("Information", "Algorithm terminated successfully!")
        else:
            messagebox.showerror("Error", "The algorithm encountered errors!")

    def view_results(self):
        self.master.switch_frame(ResultsView)


class UploadFileView(Frame):
    upload_url = 'http://localhost:5000/upload'

    def __init__(self, master):
        Frame.__init__(self, master)

        self.init_ui()

    def init_ui(self):

        self.master.title("Upload file")
        self.master.geometry("300x200")

        self.pack(fill=BOTH, expand=1)

        self.btn_select_file = Button(self, text="Select file", command=self.on_open)
        self.btn_select_file.place(x=80, y=50)

        self.selected_file_name = Label(self, text="<Selected file name>")
        self.selected_file_name.place(x=60, y=90)

        self.btn_upload_file = Button(self, text="Upload file", command=self.upload_file)
        self.btn_upload_file.place(x=80, y=130)

        self.btn_back = Button(self, text="Back", command=self.go_back)
        self.btn_back.place(x=10, y=10)

    def __set_full_path_of_file(self, value):
        self.full_path_of_file = value

    def on_open(self):

        ftypes = [('CSV', '.csv'), ('JSON', '.json'), ('All files', '*')]
        dlg = filedialog.Open(self, filetypes=ftypes)
        fl = dlg.show()

        if fl:
            file_name = fl.split('/')[len(fl.split('/')) - 1]
            self.selected_file_name.configure(text=file_name)

            self.full_path_of_file = fl
        else:
            self.selected_file_name.configure(text="<Selected file name>")
            self.__set_full_path_of_file(None)

    def upload_file(self):
        try:
            with open(self.full_path_of_file, 'rb') as f:
                r = requests.post(self.upload_url, files={'file': f})

            self.selected_file_name.configure(text="<Selected file name>")

            if r.status_code == requests.codes.ok:
                self.__set_full_path_of_file(None)
                messagebox.showinfo("Information", "File uploaded successfully!")
            else:
                messagebox.showerror("Error", "Could not upload file")
        except AttributeError as error:
            # this exception is raised when the upload was pressed but
            # no file was selected
            pass

    def go_back(self):
        self.master.switch_frame(MainView)


class RedirectOutputText:

    def __init__(self, text_ctrl):
        """Constructor"""
        self.output = text_ctrl

    def write(self, string):
        self.output.insert(END, string)

    def flush(self):
        pass


class TrainingFileView(Frame):
    uncertain_pairs_url = 'http://localhost:5000/files/uncertain_pairs_file'
    upload_url = 'http://localhost:5000/upload'

    def __init__(self, master):
        """Constructor"""
        Frame.__init__(self, master)

        self.init_UI()

    def init_UI(self):

        self.master.title("Create and upload training file")
        self.master.geometry('400x400')

        self.text_area = scrolledtext.ScrolledText(self)
        self.text_area.pack()

        self.user_input = Entry(self, width=10)
        self.user_input.pack()

        sys.stdout = RedirectOutputText(self.text_area)

        # get the uncertain pairs file from server

        self.console_label = ConsoleLabel(self.get_uncertain_pairs_file())
        self.current_record_pair = self.console_label.get_uncertain_pair()

        self.btn_next = Button(self, text="Next", bg="green", command=self.get_input)
        self.btn_next.pack()

        self.back = Button(self, text="Back", command=self.go_back)
        self.back.pack()

    def go_back(self):

        self.master.switch_frame(MainView)

    def get_input(self):
        if self.console_label is None:
            self.text_area.delete('1.0', END)
            print("The training has finished and the training file was created and sent to the server! Go Back.")
            return

        valid_responses = {'y', 'n', 'u', 'f'}

        user_input = self.user_input.get()

        self.user_input.delete(0, END)

        if user_input not in valid_responses:
            return

        self.console_label.label_record_pair(user_input, self.current_record_pair)

        if user_input == 'f':
            self.upload_training_file()
            self.current_record_pair = None
            self .console_label = None
            self.text_area.delete('1.0', END)
            return

        self.text_area.yview(END)

        self.current_record_pair = self.console_label.get_uncertain_pair()

    def get_uncertain_pairs_file(self):
        # GET a the uncertain_pairs file
        file_name = 'uncertain_pairs_file'

        response = requests.get(self.uncertain_pairs_url, stream=True)

        return response.content

    def upload_training_file(self):
        # POST a file
        file_path = os.getcwd() + "/" + self.console_label.training_file_name

        with open(file_path, 'r') as f:
            r = requests.post(self.upload_url, files={'file': f})

        if r.status_code != requests.codes.ok:
            messagebox.showerror("Error", "The training file could not be uploaded!")


class ResultsView(Frame):

    companies_url = "http://localhost:5000/search/company/name/"

    def __init__(self, master):
        """Constructor"""
        Frame.__init__(self, master)

        self.init_UI()

    def init_UI(self):

        self.master.title("Search for different companies")
        self.master.geometry("400x400")

        self.label_combobox = Label(self, text="Search by")
        self.label_combobox.pack()

        ##################################################################### TODO get selected box

        self.combo_searching_options = Combobox(self, state="readonly")
        self.combo_searching_options['values'] = ("Name",)
        self.combo_searching_options.pack()

        self.label_input = Label(self, text="Entry the value")
        self.label_input.pack()

        ####################################################################### TODO empty area before submit button
        self.user_input = Entry(self, width=40)
        self.user_input.pack()

        self.btn_submit = Button(self, text="Submit", command=self.submit)
        self.btn_submit.pack()

        self.text_area = scrolledtext.ScrolledText(self)
        self.text_area.pack()

        sys.stdout = RedirectOutputText(self.text_area)

        self.btn_back = Button(self, text="Back", command=self.go_back)
        self.btn_back.pack()

    def go_back(self):
        self.master.switch_frame(MainView)

    def submit(self):
        r = requests.get(self.companies_url + self.user_input.get(), stream=True)

        buff = BytesIO(r.content)

        companies = pickle.load(buff)

        self.print_results(companies)

        self.text_area.yview(END)

    def print_results(self, result):

        for key, values in result.items():
            print(key)
            for example in values:
                for field, value in example.items():
                    print(str(field) + ": " + str(value))
            print("\n")


def main():
    ca = ClientApp()
    ca.mainloop()


if __name__ == '__main__':
    main()
