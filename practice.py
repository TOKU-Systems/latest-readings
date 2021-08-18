def height_calculation(pres,acceleration_due_to_gravity,specific_gravity):
    height = pres/(acceleration_due_to_gravity * specific_gravity)
    return height


h=height_calculation(100,9.8,9)
print(h)
