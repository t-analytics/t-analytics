from django import forms


class SignInForm(forms.Form):
    login = forms.CharField(widget=forms.TextInput(attrs={"placeholder": "Enter your login"}), label='', required=True)
    password = forms.CharField(widget=forms.PasswordInput(attrs={'placeholder': "Enter your password"}), label='',
                               required=True)
