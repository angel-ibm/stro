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


    # Path to your FITS file
fits_file_path = './images/m31.fits'  # Replace with your FITS file path
    
    # Extract and display the FITS header
header = extract_fits_header(fits_file_path)

image_width = header['NAXIS1']
image_height = header['NAXIS2']
image_utz = header['UT-OBS']
object_name = header['OBJECT']
object_ra = header['RA']
object_dec = header['DEC']
object_alt = header['TELALT']
object_az = header['TELAZ']
camera_focus = header['CAMFOCUS']
local_temp = header['TELTEMP']
local_lat = header['LATITUDE']
local_long = header['LONGITUD']
local_weather = header['WEATHER']

print("The width of the image in bytes is:", image_width)
print("The height of the image in pixels is:", image_height)
print("The UT time of the observation is:", image_utz)
print("The name of the observed object is:", object_name)
print("The RA of the observed object is:", object_ra)
print("The DEC of the observed object is:", object_dec)
print("The altitude of the telescope at the time of observation is:", object_alt)
print("The azimuth of the telescope at the time of observation is:", object_az)
print("The camera focus setting at the time of observation is:", camera_focus)
print("The temperature of the observatory at the time of observation is:", local_temp)
print("The latitude of the observatory at the time of observation is:", local_lat)
print("The longitude of the observatory at the time of observation is:", local_long)
print("The weather conditions at the time of observation were:", local_weather)