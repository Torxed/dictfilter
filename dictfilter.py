import sys, re, os, signal, time, pickle

def sig_handler(signal, frame):
	#print(f'Exiting on line {raw_line}')
	output.flush()
	with open(f'{os.path.expanduser("~")}/.filter_dictionary/{os.path.basename(in_file_name)}.position', 'w') as tmp:
		tmp.write(str(in_file_position))
	with open(f'{os.path.expanduser("~")}/.filter_dictionary/{os.path.basename(in_file_name)}.metadata', 'wb') as tmp:
		pickle.dump({
			'uniques' : uniques,
			'skipped' : skipped,
			'bytes_saved' : bytes_saved,
			'massaged' : massaged,
			'saved' : saved,
			'start_time' : start_time
		}, tmp)
	exit(0)

signal.signal(signal.SIGINT, sig_handler)

in_file_name = sys.argv[1]

specials_thresholds = {
	1 : 100,
	2 : 50,
	3 : 25,
	4 : 25,
	5 : 20,
	6 : 30,

	0 : 30 # Default threshold
}

special_characters = re.compile(b'[^a-zA-Z]+')
words_only = re.compile(b'[a-zA-Z]+')
uniques = {}

last_output = time.time()
skipped = 0
massaged = 0
saved = 0
bytes_parsed = 0
bytes_saved = 0
bytes_total = os.stat(in_file_name).st_size
last_skipepd = None
last_parsed = None
last_massaged = None
in_file_position = 0
start_time = time.time()
restarted = False
max_thread_count = os.cpu_count()

print('')
print('[?] No output means we\'re iterating but that')
print('    we\'re only reaching skip-logic code (faster executions).')
print('')
print('    Info is written every 5 seconds from the first')
print('    non-skipped line.. Will now enter quiet mode until then!')
print('')

if not os.path.isdir(f'{os.path.expanduser("~")}/.filter_dictionary'):
	os.mkdir(f'{os.path.expanduser("~")}/.filter_dictionary')

## Load last session if there is one:
if os.path.isfile(f'{os.path.expanduser("~")}/.filter_dictionary/{os.path.basename(in_file_name)}.position'):
	with open(f'{os.path.expanduser("~")}/.filter_dictionary/{os.path.basename(in_file_name)}.position', 'r') as tmp:
		in_file_position = int(tmp.read().strip())
		bytes_parsed = in_file_position
		restarted = True
## Load metadata for the session if there is one
if os.path.isfile(f'{os.path.expanduser("~")}/.filter_dictionary/{os.path.basename(in_file_name)}.metadata'):
	with open(f'{os.path.expanduser("~")}/.filter_dictionary/{os.path.basename(in_file_name)}.metadata', 'rb') as tmp:
		tmp_data = pickle.load(tmp)
		uniques = tmp_data['uniques']
		skipped = tmp_data['skipped']
		massaged = tmp_data['massaged']
		saved = tmp_data['saved']
		start_time = tmp_data['start_time']
		bytes_saved = tmp_data['bytes_saved']

if in_file_position == 0 and os.path.isfile(in_file_name+'.filtered'):
	print(f'An existing filtered version of {in_file_name} was found. Remove or backup+remove this file before running.')
	exit(1)

def skip(boolean, line):
	global skipped
	global last_skipepd
	if boolean:
		skipped += 1
		last_skipepd = line
	return boolean

class worker(Thread):
	def __init__(self, line):
		skipped = 0
		saved = 0
		massaged = 0
		bytes_parsed = 0

		Thread.__init__(self)
		self.start()

	def run(self):
		pass

with open(in_file_name+'.filtered', 'ab') as output:
	with open(in_file_name, 'rb') as in_file:
		in_file.seek(in_file_position)
		for raw_line in in_file:
			if restarted:
				print(f'Resuming from position: {in_file_position} [{raw_line}]')
				restarted = False

			## == Speed enhancers:
			in_file_position = in_file.tell()
			bytes_parsed += len(raw_line)
			parsed_percentage = round(100/bytes_total * bytes_parsed, 2)
			line = raw_line.strip()
			line_len = len(line)
			threshold = specials_thresholds[line_len] if line_len in specials_thresholds else specials_thresholds[0]
			specials = list(special_characters.findall(line))
			words = list(words_only.findall(line))
			specials_combined = b''.join(specials)
			special_len = len(specials_combined)

			## == Skip logic:
			if skip(line_len <= 6, line):
				continue
			elif skip(special_len == len(line), line):
				continue
			elif skip(100/line_len * special_len >= threshold, line):
				continue
			## Isolate individual words, and if they're longer than 6 and
			## longer than the threshold, then convert the line into
			## multi-line version of the original parsed line. summer2018yay -> summer \n yay
			massaged_line = b''
			for word in words:
				if 100/line_len * len(word) >= (threshold/2) and len(word) > 6:
					massaged_line += word + b'\n'
			if skip(len(massaged_line) == 0, line):
				continue
			#if skip(line.isdigit(), line):
			#	continue
			#if raw_line in uniques:
			#	skipped += 1
			#	last_skipepd = line
			#	continue

			#output.write(raw_line)
			#uniques[raw_line] = True

			## == Output/Debug logic:
			bytes_saved += len(raw_line)
			if massaged_line[:-1] == line:
				saved += 1
				last_parsed = line
			else:
				massaged += 1
				last_massaged = line
				line = massaged_line[:-1]

			output.write(line+b'\n')

			if time.time() - last_output > 5:
				saved_percent_against_total = round(100/bytes_parsed*bytes_saved, 2)
				saved_percent = round(100/(saved+skipped+massaged)*saved, 2)
				massaged_percent = round(100/(saved+skipped+massaged)*massaged, 2)
				skipped_percent = round(100/(saved+skipped+massaged)*skipped, 2)

				t = time.time() - start_time #returns seconds
				days = t // 86400
				hours = t // 3600 % 24
				minutes = t // 60 % 60
				seconds = t % 60
				runtime = f'{int(days)}days, {int(hours)}:{int(minutes)}:{int(seconds)}'

				print()
				print(f'Parsed: {parsed_percentage}% [{saved_percent_against_total}% saved] ({runtime})')
				print(f'Saved: {saved:,} [{last_parsed}] [{saved_percent}% balance]')
				print(f'Massaged: {massaged:,} [{last_massaged}] [{massaged_percent}% balance]')
				print(f'Skipped: {skipped:,} [{last_skipepd}] [{skipped_percent}% balance]')
				last_output = time.time()

			#if int(100/line_len * special_len) != 0:
			#	print(line, 100/line_len * special_len)
			#	last_output = time.time()
			

			#time.sleep(0.025)

os.remove(f'{os.path.expanduser("~")}/.filter_dictionary/{os.path.basename(in_file_name)}.metadata')
os.remove(f'{os.path.expanduser("~")}/.filter_dictionary/{os.path.basename(in_file_name)}.position')