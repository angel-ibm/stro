from astropy.io import fits

# Function to extract and print the header of a FITS image file
def extract_fits_header(fits_file_path):
    # Open the FITS file
    with fits.open(fits_file_path) as hdul:
        # Access the primary HDU (Header Data Unit)
        header = hdul[0].header  # Primary header is at index 0

        # Print the header
        print("FITS Header Information:")
        print(repr(header))
        return header

if __name__ == '__main__':
    # Path to your FITS file
    fits_file_path = 'm31.fits'  # Replace with your FITS file path
    
    # Extract and display the FITS header
    header = extract_fits_header(fits_file_path)
