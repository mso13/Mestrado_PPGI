import subprocess

program_list = ['script1.py', 'script2.py', 'script3.py']

for program in program_list:
    # Executa em ordem os programas necessários para a construção
    # das bases a serem utilizadas no projeto.
    subprocess.call(['python', program])
    print("Finished:" + program)