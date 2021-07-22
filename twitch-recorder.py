import datetime
import enum
import getopt
import logging
import os
import subprocess
import sys
import shutil
import time
import langcodes
import csv
import json
import pprint

import requests
import multiprocessing
import config

streamers_ended = []


class TwitchResponseStatus(enum.Enum):
    ONLINE = 0
    OFFLINE = 1
    NOT_FOUND = 2
    UNAUTHORIZED = 3
    ERROR = 4


def load_csv_into_dict(filename):
    database_streamers = {}
    with open(filename) as csvDataFile:
        csv_reader = csv.reader(csvDataFile)
        for row in csv_reader:
            database_streamers[row[0]] = row[1]
    return database_streamers


def collect_end_record(username):
    global streamers_ended
    streamers_ended.append(username[0])

    logging.info('collect_end_record')
    logging.info(username)
    logging.info(streamers_ended)


class TwitchRecorder:
    def __init__(self):
        # global configuration
        self.ffmpeg_path = "ffmpeg"
        self.disable_ffmpeg = False
        self.refresh = 45
        self.root_path = config.root_path

        # user configuration
        self.database_filename = "streamers.csv"
        self.log_filename = "historique.csv"
        self.game = config.game
        self.quality_exclusion = ">720p30"
        self.quality = "high,best"
        self.active_streamers = []
        self.active_streamers_count = []
        self.streamers_to_record = []
        self.streamers_recording = []
        self.n_workers = multiprocessing.cpu_count()

        # twitch configuration
        self.client_id = config.client_id
        self.client_secret = config.client_secret
        self.token_url = "https://id.twitch.tv/oauth2/token?client_id=" + self.client_id + "&client_secret=" \
                         + self.client_secret + "&grant_type=client_credentials"
        self.url = "https://api.twitch.tv/helix/"
        self.access_token = self.fetch_access_token()

        # variables
        self.game_id = self.get_game_id()
        self.language = str(langcodes.find(config.language))

    def fetch_access_token(self):
        token_response = requests.post(self.token_url, timeout=15)
        token_response.raise_for_status()
        token = token_response.json()
        return token["access_token"]

    def run(self):
        pool = multiprocessing.Pool(self.n_workers)
        while True:
            self.get_streamers_to_record()
            if len(self.streamers_to_record) != 0:
                r = pool.map_async(self.record_streamer, [self.streamers_to_record[0]], callback=collect_end_record)
                self.streamers_recording.append(self.streamers_to_record[0])
            time.sleep(self.refresh)

    def process_recorded_file(self, recorded_filename, processed_filename):
        if self.disable_ffmpeg:
            logging.info("moving: %s", recorded_filename)
            shutil.move(recorded_filename, processed_filename)
        else:
            logging.info("fixing %s", recorded_filename)
            self.ffmpeg_copy_and_fix_errors(recorded_filename, processed_filename)

    def ffmpeg_copy_and_fix_errors(self, recorded_filename, processed_filename):
        try:
            subprocess.call(
                [self.ffmpeg_path, "-err_detect", "ignore_err", "-i", recorded_filename, "-c", "copy",
                 processed_filename])
            os.remove(recorded_filename)
        except Exception as e:
            logging.error(e)

    def check_user(self, username):
        info = None
        status = TwitchResponseStatus.ERROR
        try:
            headers = {"Client-ID": self.client_id, "Authorization": "Bearer " + self.access_token}
            r = requests.get(self.url + "streams/?user_login=" + username, headers=headers, timeout=15)
            r.raise_for_status()
            info = r.json()
            if info is None or not info["data"]:
                status = TwitchResponseStatus.OFFLINE
            else:
                status = TwitchResponseStatus.ONLINE
        except requests.exceptions.RequestException as e:
            if e.response:
                if e.response.status_code == 401:
                    status = TwitchResponseStatus.UNAUTHORIZED
                if e.response.status_code == 404:
                    status = TwitchResponseStatus.NOT_FOUND
        return status, info

    def check_active_streamers_from_game(self):
        info = None
        status = TwitchResponseStatus.ERROR
        try:
            headers = {"Client-ID": self.client_id, "Authorization": "Bearer " + self.access_token}
            full_url = self.url + "streams/?game_id=" + self.game_id + "&language=" + self.language + "&first=100"
            r = requests.get(full_url, headers=headers, timeout=15)
            r.raise_for_status()
            info = r.json()
            if info is None or not info["data"]:
                status = TwitchResponseStatus.OFFLINE
            else:
                status = TwitchResponseStatus.ONLINE
            self.active_streamers = [streamer_data["user_name"] for streamer_data in info["data"]]
            self.active_streamers_count = [streamer_data["viewer_count"] for streamer_data in info["data"]]
        except requests.exceptions.RequestException as e:
            if e.response:
                if e.response.status_code == 401:
                    status = TwitchResponseStatus.UNAUTHORIZED
                if e.response.status_code == 404:
                    status = TwitchResponseStatus.NOT_FOUND

    def get_game_id(self):
        info = None
        status = TwitchResponseStatus.ERROR
        try:
            headers = {"Client-ID": self.client_id, "Authorization": "Bearer " + self.access_token}
            r = requests.get(self.url + "games/?name=" + self.game, headers=headers, timeout=15)
            r.raise_for_status()
            info = r.json()
            if info is None or not info["data"]:
                status = TwitchResponseStatus.OFFLINE
            else:
                status = TwitchResponseStatus.ONLINE
        except requests.exceptions.RequestException as e:
            if e.response:
                if e.response.status_code == 401:
                    status = TwitchResponseStatus.UNAUTHORIZED
                if e.response.status_code == 404:
                    status = TwitchResponseStatus.NOT_FOUND
        return info["data"][0]["id"]

    def update_streamer_database(self):
        self.check_active_streamers_from_game()
        db = dict.fromkeys(self.active_streamers, '')

        if os.path.isfile(self.database_filename):
            database_dict = load_csv_into_dict(self.database_filename)
            keys = db.keys() | database_dict.keys()
            db = {k: database_dict.get(k, '') for k in keys}

        with open(self.database_filename, 'w') as f:
            for curr_key in sorted(db, key=lambda x: x.lower()):
                f.write("%s,%s\n" % (curr_key, db[curr_key]))
        f.close()

    def get_streamers_to_record(self):
        # they have to be whitelisted and streaming
        self.update_streamer_database()
        streamers_dict = dict.fromkeys(self.active_streamers, '')
        database_dict = load_csv_into_dict(self.database_filename)

        keys = streamers_dict.keys() | database_dict.keys()
        db = {k: database_dict.get(k, '') for k in keys}

        filtered_db = {k: v for k, v in db.items() if v != ""}

        while len(streamers_ended) != 0:
            logging.info("streamers rcding : ")
            logging.info(self.streamers_recording)
            logging.info("streamers ended idx : ")
            logging.info(streamers_ended)
            f = open(self.log_filename, "a")
            f.write(streamers_ended[0] + " - " + datetime.datetime.now().strftime("%Y-%m-%d %Hh%Mm%Ss") + '\n')
            f.close()

            self.streamers_recording.remove(streamers_ended[0])
            streamers_ended.remove(streamers_ended[0])


        self.streamers_to_record = list((set(filtered_db.keys()) & set(self.active_streamers)
                                         ^ set(self.streamers_recording))
                                        & (set(filtered_db.keys()) & set(self.active_streamers)))

        logging.info("active streamers : " + datetime.datetime.now().strftime("%Y-%m-%d %Hh%Mm%Ss"))
        logging.info(self.active_streamers)
        logging.info(self.active_streamers_count)

    def record_streamer(self, username):
        # path to recorded stream
        recorded_path = os.path.join(self.root_path, "recorded", username)
        # path to finished video, errors removed
        processed_path = os.path.join(self.root_path, "processed", username)

        # create directory for recordedPath and processedPath if not exist
        if os.path.isdir(recorded_path) is False:
            os.makedirs(recorded_path)
        if os.path.isdir(processed_path) is False:
            os.makedirs(processed_path)

        # make sure the interval to check user availability is not less than 15 seconds
        if self.refresh < 15:
            logging.warning("check interval should not be lower than 15 seconds")
            self.refresh = 15
            logging.info("system set check interval to 15 seconds")

        # fix videos from previous recording session
        try:
            # TODO before this function was for one username, have to check all vids now
            video_list = [f for f in os.listdir(recorded_path) if os.path.isfile(os.path.join(recorded_path, f))]
            if len(video_list) > 0:
                logging.info("processing previously recorded files")
            for f in video_list:
                recorded_filename = os.path.join(recorded_path, f)
                processed_filename = os.path.join(processed_path, f)
                self.process_recorded_file(recorded_filename, processed_filename)
        except Exception as e:
            logging.error(e)

        status, info = self.check_user(username)

        if status == TwitchResponseStatus.NOT_FOUND:
            logging.error("username not found, invalid username or typo")
            time.sleep(self.refresh)
        elif status == TwitchResponseStatus.ERROR:
            logging.error("%s unexpected error. will try again in 5 minutes",
                          datetime.datetime.now().strftime("%Hh%Mm%Ss"))
            time.sleep(300)
        elif status == TwitchResponseStatus.OFFLINE:
            logging.info("%s currently offline, checking again in %s seconds", username, self.refresh)
            time.sleep(self.refresh)
        elif status == TwitchResponseStatus.UNAUTHORIZED:
            logging.info("unauthorized, will attempt to log back in immediately")
            self.access_token = self.fetch_access_token()
        elif status == TwitchResponseStatus.ONLINE:
            logging.info("%s online, stream recording in session", username)
            channels = info["data"]
            channel = next(iter(channels), None)
            filename = datetime.datetime.now().strftime("%Y-%m-%d %Hh%Mm%Ss") + " - " \
                       + username + " - " + channel.get("title") + ".mp4"

            # clean filename from unnecessary characters
            filename = "".join(x for x in filename if x.isalnum() or x in [" ", "-", "_", "."])

            recorded_filename = os.path.join(recorded_path, filename)
            processed_filename = os.path.join(processed_path, filename)

            # start streamlink process
            cl = ["streamlink", "--twitch-disable-ads", "--stream-sorting-excludes", self.quality_exclusion,
                "twitch.tv/" + username, self.quality, "-o", recorded_filename]
            subprocess.call(cl)

            # FIXME For tests :
            # f = open(recorded_filename, 'w+')
            # f.close()

            logging.info("recording stream is done, processing video file")
            if os.path.exists(recorded_filename) is True:
                self.process_recorded_file(recorded_filename, processed_filename)
            else:
                logging.info("skip fixing, file not found")

            logging.info("processing is done, going back to checking...")
        return username


def main(argv):
    twitch_recorder = TwitchRecorder()
    usage_message = "twitch-recorder.py -u <username> -q <quality>"
    logging.basicConfig(filename="twitch-recorder.log", level=logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler())

    try:
        opts, args = getopt.getopt(argv, "hu:q:l:g:",
                                   ["username=", "quality=", "log=", "logging=", "game=", "language=",
                                    "disable-ffmpeg"])
    except getopt.GetoptError:
        print(usage_message)
        sys.exit(2)
    for opt, arg in opts:
        if opt == "-h":
            print(usage_message)
            sys.exit()
        elif opt in ("-g", "--game"):
            twitch_recorder.game = arg
        elif opt in "--language":
            twitch_recorder.language = arg
        elif opt in ("-u", "--username"):
            twitch_recorder.username = arg
        elif opt in ("-q", "--quality"):
            twitch_recorder.quality = arg
        elif opt in ("-l", "--log", "--logging"):
            logging_level = getattr(logging, arg.upper(), None)
            if not isinstance(logging_level, int):
                raise ValueError("invalid log level: %s" % logging_level)
            logging.basicConfig(level=logging_level)
            logging.info("logging configured to %s", arg.upper())
        elif opt == "--disable-ffmpeg":
            twitch_recorder.disable_ffmpeg = True
            logging.info("ffmpeg disabled")

    twitch_recorder.run()


if __name__ == "__main__":
    main(sys.argv[1:])
