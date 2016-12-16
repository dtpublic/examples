package com.datatorrent.tutorial.xmlparser;

import java.util.Date;

public class EmployeeBean
{
  @Override
  public String toString()
  {
    return "EmployeeBean [name=" + name + ", dept=" + dept + ", eid=" + eid + ", dateOfJoining=" + dateOfJoining
      + ", address=" + address + "]";
  }

  private String name;
  private String dept;
  private int eid;
  protected Date dateOfJoining;
  private Address address;

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  public String getDept()
  {
    return dept;
  }

  public void setDept(String dept)
  {
    this.dept = dept;
  }

  public int getEid()
  {
    return eid;
  }

  public void setEid(int eid)
  {
    this.eid = eid;
  }

  public Date getDateOfJoining()
  {
    return dateOfJoining;
  }

  public void setDateOfJoining(Date dateOfJoining)
  {
    this.dateOfJoining = dateOfJoining;
  }

  public Address getAddress()
  {
    return address;
  }

  public void setAddress(Address address)
  {
    this.address = address;
  }

  public static class Address
  {

    @Override
    public String toString()
    {
      return "Address [city=" + city + ", country=" + country + "]";
    }

    private String city;
    private String country;

    public String getCity()
    {
      return city;
    }

    public void setCity(String city)
    {
      this.city = city;
    }

    public String getCountry()
    {
      return country;
    }

    public void setCountry(String country)
    {
      this.country = country;
    }
  }
}
