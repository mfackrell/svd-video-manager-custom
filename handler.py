import json
import time
import requests
from diffusers import StableVideoDiffusionPipeline
import torch
import runpod


print("BOOTING SVD WORKER")
print("CUDA:", torch.version.cuda)
print("GPU:", torch.cuda.get_device_name(0))

pipe = StableVideoDiffusionPipeline.from_pretrained(
    "stabilityai/stable-video-diffusion-img2vid",
    torch_dtype=torch.float16
).to("cuda")

def handler(event):
    image_url = event["input"]["image_url"]

    image = requests.get(image_url, stream=True).raw

    frames = pipe(
        image,
        num_frames=25,
        decode_chunk_size=8
    ).frames[0]

    # Save video
    output_path = "/tmp/out.mp4"
    pipe.save_video(frames, output_path)

    return {
        "status": "COMPLETED",
        "video_path": output_path
    }

# 🚨 THIS LINE IS REQUIRED
runpod.serverless.start({
    "handler": handler
})
