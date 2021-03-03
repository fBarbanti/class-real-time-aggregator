import os


sink = os.path.join(os.getcwd(), 'sink')
checkpoint = os.path.join(os.getcwd(), 'checkpoint')
stream = os.path.join(os.getcwd(), 'stream')

try:
    os.makedirs(sink)
    os.makedirs(os.path.join(sink, 'sink_stream_raw'))
    os.makedirs(os.path.join(sink, 'sink_stream_modified'))
    os.makedirs(os.path.join(sink, 'sink_one_minute'))
    os.makedirs(os.path.join(sink, 'sink_fifteen_minute'))
    os.makedirs(os.path.join(sink, 'sink_one_hour'))
    os.makedirs(os.path.join(sink, 'sink_one_day'))
except:
    print("Tree sink error")

try:
    os.makedirs(checkpoint)
    os.makedirs(os.path.join(checkpoint, 'checkpoint_stream_raw'))
    os.makedirs(os.path.join(checkpoint, 'checkpoint_stream_modified'))
    os.makedirs(os.path.join(checkpoint, 'checkpoint_one_minute'))
    os.makedirs(os.path.join(checkpoint, 'checkpoint_fifteen_minute'))
    os.makedirs(os.path.join(checkpoint, 'checkpoint_one_hour'))
    os.makedirs(os.path.join(checkpoint, 'checkpoint_one_day'))
except:
    print("Tree checkpoint error")

try:
    os.makedirs(stream)
except:
    print("Directory stream error")