# example awk cmd to combine the non-first lines of all txt files in a dir to a new txt file, in shell glob expansion order
awk 'FNR > 1' *.txt > combined_data_ophys.txt

