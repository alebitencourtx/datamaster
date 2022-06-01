#!/bin/sh
Help()
{
   # Display Help
   echo
   echo "Digite a quantidade de tweets a serem extraidos. Informe um numero multiplo de 50"
   echo "Caso não informado, o programa seguirá com valor default de 100"
   echo
   echo "Syntax: start_batch_bronze_tt_tweets.sh 200"
   echo
   echo "options:"
   echo "h     Print this Help."
   echo
}

while getopts ":h" option; do
   case $option in
      h) # display Help
         Help
         exit;;
     \?) # Invalid option
         echo "Error: Invalid option"
         exit;;
   esac
done

pip install --no-cache-dir -q -r /app/requirements.txt 

python /app/twitter/bronze/batch/batch_bronze_tt_tweets.py $1; exit_code=$?

if [ $exit_code -eq 0 ]
  then
    echo ">>>>> EXECUTADO COM SUCESSO <<<<<"
  else
    echo ">>>>> OCORREU ERRO NA EXECUCAO <<<<<"
fi


exit "$exit_code"