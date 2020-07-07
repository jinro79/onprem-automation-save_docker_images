#!/usr/bin/python

import argparse
from datetime import datetime

import docker
import json
import logging
import pprint
import shutil
import subprocess
import sys
import tempfile
import os

import requests

pp = pprint.PrettyPrinter(indent=4)
# sys.tracebacklimit = 0
client = docker.from_env()
cli = docker.APIClient(base_url='unix://var/run/docker.sock')


def write_successful_run_timestamp_to_file(args):
    timestamp=datetime.now().astimezone().replace(microsecond=0).isoformat()
    with open(f"{args.output}/save_docker_images_last_successful_run.txt", 'w+') as f:
        f.seek(0)
        f.write(timestamp)
        f.truncate()
    logging.info(f"Recorded {timestamp} to {args.output}/save_docker_images_last_successful_run.txt")


def save_all_images_in_one_tar(args, images_to_save):
    images_list = " ".join(images_to_save)
    logging.info(f"Saving all docker images in a single file: {args.tar_filename}")
    completed = subprocess.run(f"docker save {images_list} -o {args.tar_filename}",
                               shell=True,
                               universal_newlines=True)
    logging.info(f"Saving all docker images to {args.tar_filename} Return code: {completed.returncode}")


def get_image_ids(images):
    image_ids = set()
    for image in images:
        _, value = image.short_id.split(":", 1)
        image_ids.add(value)

    return list(image_ids)


def get_local_image_files_names():
    images = client.images.list()
    filenames = []
    for image in images:
        for name in image.attrs.get('RepoTags'):
            filenames.append(get_filename(name))
    return filenames


def get_filename(image_name):
    file_name = image_name.replace('/', '-')
    file_name = file_name.replace(':', '-')
    file_name = file_name + ".tar"
    return file_name


def get_tars(args):
    tars = []
    for root, _, files in os.walk(args.output):
        for file in files:
            if file.endswith(".tar"):
                tars.append(file)
    return tars


def save_docker_images(args, images_to_save):
    """
    Save the images as a tar file
    :param images_to_save:
    :param args:
    :return:
    """
    for image_name in list(images_to_save):
        # file_name = image.attrs.get('RepoTags')[0]
        file_name = get_filename(image_name)
        dirname, _ = os.path.split(args.output)
        logging.debug("Writing the Temp file %s to folder %s " % (file_name, dirname))
        temp_file = tempfile.NamedTemporaryFile(prefix=file_name, dir=dirname, delete=True)
        try:
            image = client.images.get(image_name)
            f = temp_file
            logging.info("Writing %s" % file_name)
            for chunk in image.save(named=True):
                f.write(chunk)
            shutil.copy(f.name, file_name)
            logging.info("Completed writing %s" % file_name)
        except requests.exceptions.HTTPError as e:
            logging.error(e)
            pass
        except docker.errors.APIError as e:
            logging.error(e)
            pass


def pull_images(args, images_to_process, existing_tars):
    """
    Loops through a list of images to update.
        if the image pulls an update add it to the list of images to save to file.
    if there is no existing tar file for the image add it to the list of images to save to file.
    return the set of images to save to file
    :param args:
    :param images_to_process:
    :param existing_tars:
    :return: images_to_save
    """
    images_to_save = set()
    image_name: str
    if args.all_in_one:
        images_to_save_all_in_one = set()
    for image_name in images_to_process:
        try:
            logging.info("Pulling %s" % image_name)
            line: dict
            for line in cli.pull(image_name, stream=True, decode=True):
                output = json.dumps(line, indent=4)
                if "Downloaded newer image for {}".format(image_name) in output:
                    # images_to_save.add[client.images.get(image_name).short_id] = image_name
                    images_to_save.add(image_name)
                    logging.info("Added %s to images_to_save" % image_name)
                    if args.all_in_one:
                        for tag in client.images.get(image_name).tags:
                            images_to_save_all_in_one.add(tag)
                        # images_to_save_all_in_one.add(tuple(client.images.get(image_name).tags))
                logging.info(output)
                logging.info("Completed pulling %s" % image_name)
                # if get_filename(client.images.get(image_name).tags[0]) not in existing_tars:
            if get_filename(image_name) not in existing_tars:
                # images_to_save[client.images.get(image_name).short_id] = image_name
                images_to_save.add(image_name)
                logging.info("Added %s to images to save" % image_name)
            if args.force:
                images_to_save[client.images.get(image_name).short_id] = image_name
                if args.all_in_one:
                    for tag in client.images.get(image_name).tags:
                        images_to_save_all_in_one.add(tag)
                        # images_to_save_all_in_one.add(tuple(client.images.get(image_name).tags))
        except requests.exceptions.HTTPError as e:
            logging.error(e)
            pass
        except docker.errors.APIError as e:
            logging.error(e)
            pass
    if args.all_in_one:
        return list(images_to_save_all_in_one)
    return list(images_to_save)


def get_local_docker_images():
    return client.images.list()


def read_image_from_file(args):
    """
    Reads in a text file that has a list of docker images to save too file.
    :param args:
    :return:
    """
    try:
        with open(args.filename, "r") as img_file:
            img_list = img_file.readlines()
            if img_list:
                return [i.strip() for i in img_list]
            else:
                return False
    except FileNotFoundError as e:
        logging.exception("Could not find the file: %s" % e.filename)
        sys.exit(e.errno)


def init_logging(args, parser):
    # set up logging to file - see previous section for more details
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%y-%m-%d %H:%M',
                        filename="./save_docker_images%s.log" % datetime.now().strftime('_%Y%m%d_%H%M%S'),
                        filemode='w')
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)
    if args.filename is None and args.image is None:
        parser.print_help(sys.stderr)
        sys.exit(1)
    # Now, we can log to the root logger, or any other logger. First the root...
    logging.info('Initialized the logger for %s.' % parser.prog)


def main():
    parser = argparse.ArgumentParser(prog="save_docker_images.py",
                                     description="""
                                     Pulls docker images from the internet and saves them to individual files or as a 
                                     single file. If an image has multiple tags only one of them will be used for the 
                                    file name, but the tags will be preserved when loaded.
                                     """
                                     )
    parser.add_argument("-f", "--filename", type=str,
                        help="A file listing docker images to load and save as a tar file.")
    parser.add_argument("-i", "--image", nargs="+", help="List of docker images to pull and save.")
    parser.add_argument("-o", "--output", type=str, default=".",
                        help="Path to save the tar files. also the location to read in tar files. Default is ','")
    parser.add_argument("--force", dest='force', action='store_true', help="Force save image(s) to file")
    all_in_one_group = parser.add_argument_group("all-in-one")
    all_in_one_group.add_argument("-a", "--all_in_one", dest='all_in_one', action='store_true',
                                help="Flag to save all images in a single tar file.")
    all_in_one_group.set_defaults(all_in_one=False)
    all_in_one_group.add_argument("--tar_filename", type=str, default="all_in_one.tar",
                                help="Path and File name for the all in one tar.")

    args = parser.parse_args()
    init_logging(args, parser)

    images_to_save = []
    images_to_process = []
    # read the file
    if args.filename is not None:
        images_to_process = read_image_from_file(args)
    # add any images that were added with image argument
    if args.image:
        logging.info("Adding the images passed as parameters to the images to process.")
        images_to_process.extend(args.image)
    else:
        logging.info("No images were passed through the command line.")

    existing_tars = get_tars(args)
    # local_images_file_names = get_local_image_files_names()

    # Pull images from the remote registry
    images_to_save.extend(pull_images(args, images_to_process, existing_tars))

    if len(images_to_save) < 1:
        logging.info("There are no images to save.")
    elif args.all_in_one:
        logging.info("All in one flag is set to true and attempting to save all images as a single file.")
        save_all_images_in_one_tar(args, images_to_save)
    else:
        # save images as tars
        save_docker_images(args, images_to_save)

    write_successful_run_timestamp_to_file(args)

    logging.info(f"End of {os.path.basename(__file__)}")


# Run the main() function
if __name__ == '__main__':
    main()
