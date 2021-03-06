import sys
import os

from tkinter import Frame, Tk, BOTH, Button, Label, messagebox, scrolledtext, Entry, END
from tkinter import filedialog
from tkinter.ttk import Combobox

import requests
import pickle

from console_label import ConsoleLabel
from io import BytesIO

if os.getenv('HTTP_HOST'):
    root_url = os.getenv('HTTP_HOST')
else:
    root_url = 'http://localhost:5000'

class ClientApp(Tk):

    def __init__(self):
        Tk.__init__(self)
        self._frame = None
        self.switch_frame(MainView)

    def switch_frame(self, frame_class):
        """
            Destroys current frame and replaces it with a new one.

            :param frame_class: frame object representing the new frame
        """
        new_frame = frame_class(self)

        if self._frame is not None:
            self._frame.destroy()

        self._frame = new_frame
        self._frame.pack()


class MainView(Frame):
    run_algorithm_url = root_url + '/run_algorithm'

    def __init__(self, master):
        """
            Constructor
        """
        Frame.__init__(self, master)

        self.init_ui()

    def init_ui(self):
        """
            This function creates and initializes widgets which are specific for this panel.
        """
        self.master.title("Backbone")
        self.master.geometry("300x150")

        self.pack(fill=BOTH, expand=1)

        self.btn_upload_file = Button(self, text="Upload file", command=self.upload_file)
        self.btn_upload_file.place(x=90, y=10)

        self.btn_create_training_file = Button(self, text="Create & upload training file",
                                               command=self.create_training_file)
        self.btn_create_training_file.place(x=30, y=40)

        self.btn_run_algorithm = Button(self, text="Run algorithm", command=self.run_algorithm)
        self.btn_run_algorithm.place(x=80, y=70)

        self.btn_view_results = Button(self, text="View Results", command=self.view_results)
        self.btn_view_results.place(x=85, y=100)

    def upload_file(self):
        """
            This function changes the "main" frame to "upload file" frame.
        """
        self.master.switch_frame(UploadFileView)

    def create_training_file(self):
        """
            This function changes the "main" frame to "create and upload training file" frame.
        """
        self.master.switch_frame(TrainingFileView)

    def run_algorithm(self):
        """
            This function makes a POST request in order to run the main algorithm (jupyter notebook file).
            It prints in a message box if the algorithm terminated successfully or not.
        """
        messagebox.showinfo("Information",
                            "Be patient! It will take a while... (Press OK to really start the algorithm :)")

        r = requests.post(self.run_algorithm_url)

        if r.status_code == requests.codes.ok:
            messagebox.showinfo("Information", "Algorithm terminated successfully!")
        else:
            messagebox.showerror("Error", "The algorithm encountered errors!")

    def view_results(self):
        """
            This function changes the "main" frame to "view results" frame.
        """
        self.master.switch_frame(ResultsView)


class UploadFileView(Frame):
    upload_url = root_url + '/upload'

    def __init__(self, master):
        """
            Constructor
        """
        Frame.__init__(self, master)

        self.init_ui()

    def init_ui(self):
        """
            This function creates and initializes widgets which are specific for this panel.
        """

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
        """
            This function sets the absolute path of the file that the user
            wants to upload to the server

            :param value: absolute path of the file to be uploaded
        """
        self.full_path_of_file = value

    def on_open(self):
        """
        This function is called when the 'Select file' button is pressed. The function
        opens a file dialog, where the user can browse the file system and select a file.
        After the user selected a file and opened it, the function displays in a label
        the file name and stores in instance variables the full path of the file and its
        name
        """

        ftypes = [('CSV', '.csv'), ('JSON', '.json'), ('All files', '*')]
        dlg = filedialog.Open(self, filetypes=ftypes)

        absolute_file_path = dlg.show()
        
        if absolute_file_path:
            # extract the file name from the absolute path
            file_name = absolute_file_path.split('/')[len(absolute_file_path.split('/')) - 1]
            
            # update the label text
            self.selected_file_name.configure(text=file_name)

            self.__set_full_path_of_file(absolute_file_path)
        else:
            # update the label text
            self.selected_file_name.configure(text="<Selected file name>")

            self.__set_full_path_of_file(None)

    def upload_file(self):
        """
            This function is called when the 'Upload file' button was pressed.
            It makes a POST request for uploading the previously selected file
            and updates the label to '<Selected file name>' and also
            updates the 'full_path_of_file' instance variable to 
            'None', if the file was successfully uploaded 
        """
        
        try:
            with open(self.full_path_of_file, 'rb') as f:
                r = requests.post(self.upload_url, files={'file': f})

            self.selected_file_name.configure(text="<Selected file name>")

            if r.status_code == requests.codes.ok:
                self.__set_full_path_of_file(None)
                messagebox.showinfo("Information", "File uploaded successfully!")
            else:
                messagebox.showerror("Error", "Could not upload file")
        except AttributeError:
            # this exceptions is raised when the 'Upload file' button was pressed but
            # no file was previously selected
            pass
        except TypeError:
            # this exceptions is raised when the 'Upload file' button was pressed 
            # after the user already uploaded a file. Now a new file shoud be selected
            # and uploaded or just go Back to the main menu
            pass

    def go_back(self):
        """
            This function changes the current frame to the "main" frame.
        """
        self.master.switch_frame(MainView)


class RedirectOutputText:

    def __init__(self, text_output):
        """
            Constructor

            :param text_output : object representing where the text is written
        """
        self.output = text_output

    def write(self, string):
        self.output.insert(END, string)

    def flush(self):
        pass


class TrainingFileView(Frame):
    create_uncertain_pairs_file_url = root_url + '/create_uncertain_pairs_file'
    get_uncertain_pairs_file_url = root_url + '/files/uncertain_pairs_file'
    upload_url = root_url + '/upload'

    def __init__(self, master):
        """Constructor"""
        Frame.__init__(self, master)

        self.init_UI()

    def init_UI(self):
        """
             This function creates and initializes widgets which are specific for this panel.
        """

        self.master.title("Create and upload training file")
        self.master.geometry('400x400')

        self.text_area = scrolledtext.ScrolledText(self)
        self.text_area.pack()

        self.user_input = Entry(self, width=10)
        self.user_input.pack()

        sys.stdout = RedirectOutputText(self.text_area)

        self.create_uncertain_pairs_file()

        self.console_label = ConsoleLabel(self.get_uncertain_pairs_file())
        self.current_record_pair = self.console_label.get_uncertain_pair()

        self.btn_next = Button(self, text="Next", bg="green", command=self.get_input)
        self.btn_next.pack()

        self.back = Button(self, text="Back", command=self.go_back)
        self.back.pack()

    def go_back(self):
        """
            This function changes the current frame to the "main" frame.
        """

        self.master.switch_frame(MainView)

    def get_input(self):
        """
             This function creates the training file. It gets a new uncertain pair object and it labels the pair with
             the input from the user. The user can give only these labels: y(yes), n(no), u(unsure), f(finished). In
             case of f(finished) a new training file is created and uploaded.
        """
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
            self.console_label = None
            self.text_area.delete('1.0', END)
            return

        self.text_area.yview(END)

        self.current_record_pair = self.console_label.get_uncertain_pair()

    def create_uncertain_pairs_file(self):
        """
            This function makes a POST request in order to create the uncertain pairs file.
        """

        response = requests.post(self.create_uncertain_pairs_file_url)

        if response.status_code != requests.codes.ok:
            messagebox.showerror("Error", "The uncertain pairs file could not be created!")

    def get_uncertain_pairs_file(self):
        """
            This function makes a GET request in order to get the uncertain pairs file.

            :return: an object which represents the response of the request.
        """

        response = requests.get(self.get_uncertain_pairs_file_url, stream=True)

        return response.content

    def upload_training_file(self):
        """
           This function makes a POST request in order to upload the new created training file.
        """

        file_path = os.getcwd() + "/" + self.console_label.training_file_name

        with open(file_path, 'r') as f:
            r = requests.post(self.upload_url, files={'file': f})

        if r.status_code != requests.codes.ok:
            messagebox.showerror("Error", "The training file could not be uploaded!")


class ResultsView(Frame):
    companies_url = root_url + "/search/company/"
    combobox_values = ("legal_name", "thoroughfare")

    def __init__(self, master):
        """
            Constructor
        """
        Frame.__init__(self, master)

        self.init_UI()

    def init_UI(self):
        """
             This function creates and initializes widgets which are specific for this panel.
        """

        self.master.title("Search for different companies")
        self.master.geometry("400x400")

        self.label_combobox = Label(self, text="Search by")
        self.label_combobox.pack()

        self.combo_searching_options = Combobox(self, state="readonly")
        self.combo_searching_options['values'] = self.combobox_values
        self.combo_searching_options.pack()

        self.label_input = Label(self, text="Entry the value")
        self.label_input.pack()

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
        """
            This function changes the current frame to the "main" frame.
        """
        self.master.switch_frame(MainView)

    def submit(self):
        """
            This function makes a GET request in order to get the queried results. The results represent a
            dictionary which contains examples from the database.
        """

        if self.text_area.get(1.0, END) is not None:
            self.text_area.delete(1.0, END)

        value_combo_box = self.combo_searching_options.get()

        r = requests.get(self.companies_url + value_combo_box + "/" + self.user_input.get(), stream=True)

        buff = BytesIO(r.content)

        companies = pickle.load(buff)

        self.print_results(companies)

        self.text_area.yview(END)

    def print_results(self, result):
        """
            This function prints the results in a pretty way. :)

            :param result: a dictionary object where the key is the provider name and the values are also a dictionary
                           containing rows from the database represented by the fields and their values
        """
        if result:
            for key, values in result.items():
                print("From provider " + key + "\n")
                for example in values:
                    for field, value in example.items():
                        print(str(field) + ": " + str(value))
                    print("\n")
                print("\n")

        else:
            print("No data has been found for your search.")


def main():
    ca = ClientApp()
    ca.mainloop()


if __name__ == '__main__':
    main()
