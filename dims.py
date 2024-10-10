from astropy.io import fits

# Function to get the dimensions of the FITS image
def get_fits_image_dimensions(fits_file_path):
    # Open the FITS file
    with fits.open(fits_file_path) as hdul:
        # Assuming the image data is in the primary HDU (index 0)
        image_data = hdul[0].data

        # If there's image data, return the shape (dimensions)
        if image_data is not None:
            dimensions = image_data.shape
            return dimensions
        else:
            print("No image data found in the FITS file.")
            return None

if __name__ == '__main__':
    # Path to your FITS file
    fits_file_path = 'm31.fits'  # Replace with actual file path
    
    # Get dimensions of the FITS image
    dimensions = get_fits_image_dimensions(fits_file_path)

    if dimensions:
        print(f"Dimensions of the FITS image: {dimensions}")
    else:
        print("Could not obtain dimensions.")
